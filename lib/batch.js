
// TODO think about how to tune these
var DEFAULT_TIMEOUT_MILLIS = 100;
var DEFAULT_BATCH_SIZE = 30; 

function Batch(keen, options){ 
  
  options = options || {};
  
  var timeoutMillis = options.timeoutMillis || DEFAULT_TIMEOUT_MILLIS,
      batchSize = options.batchSize || DEFAULT_BATCH_SIZE,
      queue   = [],
      pending = false, 
      timeoutId = null,
      flushRequested = false;
  
  function flush(){
    if(pending){ 
      flushRequested = true;
      return;
    }
    flushRequested = false;
    
    var events = {}; // coll => list of events 
    var batch  = queue.slice(0, batchSize);
    batch.forEach(function(req){
      var list = events[req.collection] = (events[req.collection] || []); 
      req.index = list.length;
      list.push(req.event);
    });
    
    pending = true;
    clearTimeout(timeoutId);
    
    keen.addEvents(events, function(err, res){ // TODO deal with error
      pending = false;
      update();
      
      batch.forEach(function(req){
        var result = res[req.collection][req.index]; // TODO deal with garbage res
        req.callback(null ,result);
      })
    });
  }
  
  
  function update(){
    if(!pending){
      if(queue.length >= batchSize || flushRequested){
        flush();
      }else if(queue.length != 0){
        clearTimeout(timeoutId);
        timeoutId = setTimeout(flush, timeoutMillis);
      }else{
        clearTimeout(timeoutId);
      }
    }
  }
  
  function addEvent(collection, event, callback){
    clearTimeout(timeoutId);
    queue.push({collection:collection, event:event, callback:callback});
    update();
  }

  Object.defineProperties(this, {
    flush: {value: flush},
    addEvent: {value: addEvent},
    batchSize: {
      get: function(){ return batchSize; },
      set: function(v){ batchSize = v; update(); }
    },
    timeoutMillis: {
      get: function(){ return timeoutMillis; },
      set: function(v){ timeoutMillis = v; update(); }
    }
  });
}

module.exports = Batch;