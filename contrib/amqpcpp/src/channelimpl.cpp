/**
 *  Channel.cpp
 *
 *  Implementation for a channel
 *
 *  @copyright 2014 - 2018 Copernica BV
 */
#include "includes.h"
#include "basicgetokframe.h"
#include "basicreturnframe.h"
#include "consumedmessage.h"
#include "returnedmessage.h"
#include "channelopenframe.h"
#include "channelflowframe.h"
#include "channelcloseokframe.h"
#include "channelcloseframe.h"
#include "confirmselectframe.h"
#include "transactionselectframe.h"
#include "transactioncommitframe.h"
#include "transactionrollbackframe.h"
#include "exchangedeclareframe.h"
#include "exchangedeleteframe.h"
#include "exchangebindframe.h"
#include "exchangeunbindframe.h"
#include "queuedeclareframe.h"
#include "queuebindframe.h"
#include "queueunbindframe.h"
#include "queuepurgeframe.h"
#include "queuedeleteframe.h"
#include "basicpublishframe.h"
#include "basicheaderframe.h"
#include "bodyframe.h"
#include "basicqosframe.h"
#include "basicconsumeframe.h"
#include "basiccancelframe.h"
#include "basicackframe.h"
#include "basicnackframe.h"
#include "basicrecoverframe.h"
#include "basicrejectframe.h"
#include "basicgetframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Constructor
 */
ChannelImpl::ChannelImpl() = default;

/**
 *  Destructor
 */
ChannelImpl::~ChannelImpl()
{
    // remove this channel from the connection (but not if the connection is already destructed)
    if (_connection) _connection->remove(this);
}

/**
 *  Callback that is called when an error occurs.
 *
 *  Only one error callback can be registered. Calling this function
 *  multiple times will remove the old callback.
 *
 *  @param  callback    the callback to execute
 */
void ChannelImpl::onError(const ErrorCallback &callback)
{
    // store callback
    _errorCallback = callback;

    // if the channel is usable, all is ok
    if (usable()) return;

    // validity check
    if (!callback) return;

    // is the channel closing down?
    if (_state == state_closing) return callback("Channel is closing down");

    // the channel is closed, but what is the connection doing?
    if (_connection == nullptr) return callback("Channel is not linked to a connection");
    
    // if the connection is valid, this is a pure channel error
    if (_connection->ready()) return callback("Channel is in an error state, but the connection is valid");

    // the connection is closing down
    if (_connection->closing()) return callback("Channel is in an error state, the AMQP connection is closing down");

    // the connection is already closed
    if (_connection->closed()) return callback("Channel is in an error state, the AMQP connection has been closed");
   
    // direct call if channel is already in error state
    callback("Channel is in error state, something went wrong with the AMQP connection");
}

/**
 *  Initialize the object with an connection
 *  @param  connection
 *  @return bool
 */
bool ChannelImpl::attach(Connection *connection)
{
    // get connection impl
    _connection = &connection->_implementation;
    
    // retrieve an ID
    _id = _connection->add(shared_from_this());
    
    // check if the id is valid
    if (_id == 0)
    {
        // this is invalid
        _state = state_closed;
        
        // failure
        return false;
    }
    else 
    {
        // assume channel is connected
        _state = state_connected;
    
        // send the open frame
        if (send(ChannelOpenFrame(_id))) return true;

        // this is an error
        _state = state_closed;
        
        // report failure
        return false;
    }
}    

/**
 *  Push a deferred result
 *  @param  result          The deferred object to push
 */
Deferred &ChannelImpl::push(const std::shared_ptr<Deferred> &deferred)
{
    // do we already have an oldest?
    if (!_oldestCallback) _oldestCallback = deferred;

    // do we already have a newest?
    if (_newestCallback) _newestCallback->add(deferred);

    // store newest callback
    _newestCallback = deferred;

    // done
    return *deferred;
}

/**
 *  Send a frame and push a deferred result
 *  @param  frame           The frame to send
 */
Deferred &ChannelImpl::push(const Frame &frame)
{
    // send the frame, and push the result
    return push(std::make_shared<Deferred>(!send(frame)));
}

/**
 *  Pause deliveries on a channel
 *
 *  This will stop all incoming messages
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::pause()
{
    // send a channel flow frame
    return push(ChannelFlowFrame(_id, false));
}

/**
 *  Resume a paused channel
 *
 *  This will resume incoming messages
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::resume()
{
    // send a channel flow frame
    return push(ChannelFlowFrame(_id, true));
}

/**
 *  Put channel in a confirm mode
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
DeferredConfirm &ChannelImpl::confirmSelect()
{
    // the frame to send
    ConfirmSelectFrame frame(_id);

    // send the frame, and create deferred object
    _confirm = std::make_shared<DeferredConfirm>(!send(frame));

    // push to list
    push(_confirm);

    // done
    return *_confirm;
}

/**
 *  Start a transaction
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::startTransaction()
{
    // send a transaction frame
    return push(TransactionSelectFrame(_id));
}

/**
 *  Commit the current transaction
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::commitTransaction()
{
    // send a transaction frame
    return push(TransactionCommitFrame(_id));
}

/**
 *  Rollback the current transaction
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::rollbackTransaction()
{
    // send a transaction frame
    return push(TransactionRollbackFrame(_id));
}

/**
 *  Close the current channel
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::close()
{
    // this is completely pointless if already closed
    if (!usable()) return push(std::make_shared<Deferred>(_state == state_closing));
    
    // send a channel close frame
    auto &handler = push(ChannelCloseFrame(_id));

    // was the frame sent and are we still alive?
    if (handler) _state = state_closing;

    // done
    return handler;
}

/**
 *  declare an exchange

 *  @param  name        name of the exchange to declare
 *  @param  type        type of exchange
 *  @param  flags       additional settings for the exchange
 *  @param  arguments   additional arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::declareExchange(const std::string &name, ExchangeType type, int flags, const Table &arguments)
{
    // convert exchange type
    const char *exchangeType = "";
    
    // convert the exchange type into a string
    if      (type == ExchangeType::fanout)          exchangeType = "fanout";
    else if (type == ExchangeType::direct)          exchangeType = "direct";
    else if (type == ExchangeType::topic)           exchangeType = "topic";
    else if (type == ExchangeType::headers)         exchangeType = "headers";
    else if (type == ExchangeType::consistent_hash) exchangeType = "x-consistent-hash";

    // the boolean options
    bool passive = (flags & AMQP::passive) != 0;
    bool durable = (flags & AMQP::durable) != 0;
    bool autodelete = (flags & AMQP::autodelete) != 0;
    bool internal = (flags & AMQP::internal) != 0;
    bool nowait = (flags & AMQP::nowait) != 0;

    // send declare exchange frame
    return push(ExchangeDeclareFrame(_id, name, exchangeType, passive, durable, autodelete, internal, nowait, arguments));
}

/**
 *  bind an exchange
 *
 *  @param  source      exchange which binds to target
 *  @param  target      exchange to bind to
 *  @param  routingKey  routing key
 *  @param  arguments   additional arguments for binding
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::bindExchange(const std::string &source, const std::string &target, const std::string &routingkey, const Table &arguments)
{
    // send exchange bind frame
    return push(ExchangeBindFrame(_id, target, source, routingkey, false, arguments));
}

/**
 *  unbind two exchanges
 *
 *  @param  source      the source exchange
 *  @param  target      the target exchange
 *  @param  routingkey  the routing key
 *  @param  arguments   additional unbind arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::unbindExchange(const std::string &source, const std::string &target, const std::string &routingkey, const Table &arguments)
{
    // send exchange unbind frame
    return push(ExchangeUnbindFrame(_id, target, source, routingkey, false, arguments));
}

/**
 *  remove an exchange
 *
 *  @param  name        name of the exchange to remove
 *  @param  flags       additional settings for deleting the exchange
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::removeExchange(const std::string &name, int flags)
{
    // send delete exchange frame
    return push(ExchangeDeleteFrame(_id, name, (flags & ifunused) != 0, false));
}

/**
 *  declare a queue
 *  @param  name        queue name
 *  @param  flags       additional settings for the queue
 *  @param  arguments   additional arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
DeferredQueue &ChannelImpl::declareQueue(const std::string &name, int flags, const Table &arguments)
{
    // the frame to send
    QueueDeclareFrame frame(_id, name, (flags & passive) != 0, (flags & durable) != 0, (flags & exclusive) != 0, (flags & autodelete) != 0, false, arguments);

    // send the queuedeclareframe
    auto result = std::make_shared<DeferredQueue>(!send(frame));

    // add the deferred result
    push(result);

    // done
    return *result;
}

/**
 *  Bind a queue to an exchange
 *
 *  @param  exchangeName    name of the exchange to bind to
 *  @param  queueName       name of the queue
 *  @param  routingkey      routingkey
 *  @param  arguments       additional arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::bindQueue(const std::string &exchangeName, const std::string &queueName, const std::string &routingkey, const Table &arguments)
{
    // send the bind queue frame
    return push(QueueBindFrame(_id, queueName, exchangeName, routingkey, false, arguments));
}

/**
 *  Unbind a queue from an exchange
 *
 *  @param  exchange    the source exchange
 *  @param  queue       the target queue
 *  @param  routingkey  the routing key
 *  @param  arguments   additional bind arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::unbindQueue(const std::string &exchange, const std::string &queue, const std::string &routingkey, const Table &arguments)
{
    // send the unbind queue frame
    return push(QueueUnbindFrame(_id, queue, exchange, routingkey, arguments));
}

/**
 *  Purge a queue
 *  @param  queue       queue to purge
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 *
 *  The onSuccess() callback that you can install should have the following signature:
 *
 *      void myCallback(AMQP::Channel *channel, uint32_t messageCount);
 *
 *  For example: channel.declareQueue("myqueue").onSuccess([](AMQP::Channel *channel, uint32_t messageCount) {
 *
 *      std::cout << "Queue purged, all " << messageCount << " messages removed" << std::endl;
 *
 *  });
 */
DeferredDelete &ChannelImpl::purgeQueue(const std::string &name)
{
    // the frame to send
    QueuePurgeFrame frame(_id, name, false);

    // send the frame, and create deferred object
    auto deferred = std::make_shared<DeferredDelete>(!send(frame));

    // push to list
    push(deferred);

    // done
    return *deferred;
}

/**
 *  Remove a queue
 *  @param  queue       queue to remove
 *  @param  flags       additional flags
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 *
 *  The onSuccess() callback that you can install should have the following signature:
 *
 *      void myCallback(AMQP::Channel *channel, uint32_t messageCount);
 *
 *  For example: channel.declareQueue("myqueue").onSuccess([](AMQP::Channel *channel, uint32_t messageCount) {
 *
 *      std::cout << "Queue deleted, along with " << messageCount << " messages" << std::endl;
 *
 *  });
 */
DeferredDelete &ChannelImpl::removeQueue(const std::string &name, int flags)
{
    // the frame to send
    QueueDeleteFrame frame(_id, name, (flags & ifunused) != 0, (flags & ifempty) != 0, false);

    // send the frame, and create deferred object
    auto deferred = std::make_shared<DeferredDelete>(!send(frame));

    // push to list
    push(deferred);

    // done
    return *deferred;
}

/**
 *  Publish a message to an exchange
 *
 *  @param  exchange    the exchange to publish to
 *  @param  routingkey  the routing key
 *  @param  envelope    the full envelope to send
 *  @param  message     the message to send
 *  @param  size        size of the message
 *  @param  flags
 *  @return DeferredPublisher
 */
DeferredPublisher &ChannelImpl::publish(const std::string &exchange, const std::string &routingKey, const Envelope &envelope, int flags)
{
    // we are going to send out multiple frames, each one will trigger a call to the handler,
    // which in turn could destruct the channel object, we need to monitor that
    Monitor monitor(this);

    // @todo do not copy the entire buffer to individual frames
    
    // make sure we have a deferred object to return
    if (!_publisher) _publisher.reset(new DeferredPublisher(this));

    // send the publish frame
    if (!send(BasicPublishFrame(_id, exchange, routingKey, (flags & mandatory) != 0, (flags & immediate) != 0))) return *_publisher;

    // channel still valid?
    if (!monitor.valid()) return *_publisher;

    // send header
    if (!send(BasicHeaderFrame(_id, envelope))) return *_publisher;

    // channel and connection still valid?
    if (!monitor.valid() || !_connection) return *_publisher;

    // the max payload size is the max frame size minus the bytes for headers and trailer
    uint32_t maxpayload = _connection->maxPayload();
    uint64_t bytessent = 0;

    // the buffer
    const char *data = envelope.body();
    uint64_t bytesleft = envelope.bodySize();

    // split up the body in multiple frames depending on the max frame size
    while (bytesleft > 0)
    {
        // size of this chunk
        uint64_t chunksize = std::min(static_cast<uint64_t>(maxpayload), bytesleft);

        // send out a body frame
        if (!send(BodyFrame(_id, data + bytessent, (uint32_t)chunksize))) return *_publisher;

        // channel still valid?
        if (!monitor.valid()) return *_publisher;

        // update counters
        bytessent += chunksize;
        bytesleft -= chunksize;
    }

    // done
    return *_publisher;
}

/**
 *  Set the Quality of Service (QOS) for this channel
 *  @param  prefetchCount       maximum number of messages to prefetch
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 * 
 *  @param  prefetchCount       number of messages to fetch
 *  @param  global              share counter between all consumers on the same channel
 */
Deferred &ChannelImpl::setQos(uint16_t prefetchCount, bool global)
{
    // send a qos frame
    return push(BasicQosFrame(_id, prefetchCount, global));
}

/**
 *  Tell the RabbitMQ server that we're ready to consume messages
 *  @param  queue               the queue from which you want to consume
 *  @param  tag                 a consumer tag that will be associated with this consume operation
 *  @param  flags               additional flags
 *  @param  arguments           additional arguments
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 *
 *  The onSuccess() callback that you can install should have the following signature:
 *
 *      void myCallback(AMQP::Channel *channel, const std::string& tag);
 *
 *  For example: channel.declareQueue("myqueue").onSuccess([](AMQP::Channel *channel, const std::string& tag) {
 *
 *      std::cout << "Started consuming under tag " << tag << std::endl;
 *
 *  });
 */
DeferredConsumer& ChannelImpl::consume(const std::string &queue, const std::string &tag, int flags, const Table &arguments)
{
    // the frame to send
    BasicConsumeFrame frame(_id, queue, tag, (flags & nolocal) != 0, (flags & noack) != 0, (flags & exclusive) != 0, false, arguments);

    // send the frame, and create deferred object
    auto deferred = std::make_shared<DeferredConsumer>(this, !send(frame));

    // push to list
    push(deferred);

    // done
    return *deferred;
}

/**
 *  Cancel a running consumer
 *  @param  tag                 the consumer tag
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 *
 *  The onSuccess() callback that you can install should have the following signature:
 *
 *      void myCallback(const std::string& tag);
 *
 *  For example: channel.declareQueue("myqueue").onSuccess([](const std::string& tag) {
 *
 *      std::cout << "Started consuming under tag " << tag << std::endl;
 *
 *  });
 */
DeferredCancel &ChannelImpl::cancel(const std::string &tag)
{
    // the cancel frame to send
    BasicCancelFrame frame(_id, tag, false);

    // send the frame, and create deferred object
    auto deferred = std::make_shared<DeferredCancel>(this, !send(frame));

    // push to list
    push(deferred);

    // done
    return *deferred;
}

/**
 *  Retrieve a single message from RabbitMQ
 * 
 *  When you call this method, you can get one single message from the queue (or none
 *  at all if the queue is empty). The deferred object that is returned, should be used
 *  to install a onEmpty() and onSuccess() callback function that will be called
 *  when the message is consumed and/or when the message could not be consumed.
 * 
 *  The following flags are supported:
 * 
 *      -   noack               if set, consumed messages do not have to be acked, this happens automatically
 * 
 *  @param  queue               name of the queue to consume from
 *  @param  flags               optional flags
 * 
 *  The object returns a deferred handler. Callbacks can be installed 
 *  using onSuccess(), onEmpty(), onError() and onFinalize() methods.
 * 
 *  The onSuccess() callback has the following signature:
 * 
 *      void myCallback(const Message &message, uint64_t deliveryTag, bool redelivered);
 * 
 *  For example: channel.get("myqueue").onSuccess([](const Message &message, uint64_t deliveryTag, bool redelivered) {
 * 
 *      std::cout << "Message fetched" << std::endl;
 * 
 *  }).onEmpty([]() {
 * 
 *      std::cout << "Queue is empty" << std::endl;
 * 
 *  });
 */
DeferredGet &ChannelImpl::get(const std::string &queue, int flags)
{
    // the get frame to send
    BasicGetFrame frame(_id, queue, (flags & noack) != 0);
    
    // send the frame, and create deferred object
    auto deferred = std::make_shared<DeferredGet>(this, !send(frame));

    // push to list
    push(deferred);

    // done
    return *deferred;
}

/**
 *  Acknowledge a message
 *  @param  deliveryTag         the delivery tag
 *  @param  flags               optional flags
 *  @return bool
 */
bool ChannelImpl::ack(uint64_t deliveryTag, int flags)
{
    // send an ack frame
    return send(BasicAckFrame(_id, deliveryTag, (flags & multiple) != 0));
}

/**
 *  Reject a message
 *  @param  deliveryTag         the delivery tag
 *  @param  flags               optional flags
 *  @return bool
 */
bool ChannelImpl::reject(uint64_t deliveryTag, int flags)
{
    // should we reject multiple messages?
    if (flags & multiple)
    {
        // send a nack frame
        return send(BasicNackFrame(_id, deliveryTag, true, (flags & requeue) != 0));
    }
    else
    {
        // send a reject frame
        return send(BasicRejectFrame(_id, deliveryTag, (flags & requeue) != 0));
    }
}

/**
 *  Recover un-acked messages
 *  @param  flags               optional flags
 *
 *  This function returns a deferred handler. Callbacks can be installed
 *  using onSuccess(), onError() and onFinalize() methods.
 */
Deferred &ChannelImpl::recover(int flags)
{
    // send a nack frame
    return push(BasicRecoverFrame(_id, (flags & requeue) != 0));
}

/**
 *  Send a frame over the channel
 *  @param  frame       frame to send
 *  @return bool        was the frame sent?
 */
bool ChannelImpl::send(const Frame &frame)
{
    // skip if channel is not connected
    if (_state == state_closed || !_connection) return false;

    // if we're busy closing, we pretend that the send operation was a
    // success. this causes the deferred object to be created, and to be
    // added to the list of deferred objects. it will be notified about
    // the error when the close operation succeeds
    if (_state == state_closing) return true;
    
    // are we currently in synchronous mode or are there
    // other frames waiting for their turn to be sent?
    if (_synchronous || !_queue.empty())
    {
        // we need to wait until the synchronous frame has
        // been processed, so queue the frame until it was
        _queue.emplace(frame.synchronous(), frame);

        // it was of course not actually sent but we pretend
        // that it was, because no error occured
        return true;
    }

    // send to tcp connection
    if (!_connection->send(frame)) return false;
    
    // frame was sent, if this was a synchronous frame, we now have to wait
    _synchronous = frame.synchronous();
    
    // done
    return true;
}

/**
 *  Signal the channel that a synchronous operation was completed. After 
 *  this operation, waiting frames can be sent out.
 */
void ChannelImpl::flush()
{
    // we are no longer waiting for synchronous operations
    _synchronous = false;

    // we need to monitor the channel for validity
    Monitor monitor(this);

    // send all frames while not in synchronous mode
    while (_connection && !_synchronous && !_queue.empty())
    {
        // retrieve the first buffer and synchronous
        auto &pair = _queue.front();

        // mark as synchronous if necessary
        _synchronous = pair.first;

        // send it over the connection
        _connection->send(std::move(pair.second));

        // the user space handler may have destructed this channel object
        if (!monitor.valid()) return;

        // remove from the list
        _queue.pop();
    }
}

/**
 *  Report an error message on a channel
 *  @param  message             the error message
 *  @param  notifyhandler       should the channel-wide handler also be called?
 */
void ChannelImpl::reportError(const char *message, bool notifyhandler)
{
    // change state
    _state = state_closed;
    _synchronous = false;
    
    // the queue of messages that still have to sent can be emptied now
    // (we do this by moving the current queue into an unused variable)
    auto queue(std::move(_queue));

    // we are going to call callbacks that could destruct the channel
    Monitor monitor(this);

    // call the oldest
    if (_oldestCallback)
    {
        // copy the callback (so that it can not be destructed during
        // the "reportError" call
        auto cb = _oldestCallback;
        
        // call the callback
        auto next = cb->reportError(message);

        // leap out if channel no longer exists
        if (!monitor.valid()) return;

        // set the oldest callback
        _oldestCallback = next;
    }

    // clean up all deferred other objects
    while (_oldestCallback)
    {
        // copy the callback (so that it can not be destructed during
        // the "reportError" call
        auto cb = _oldestCallback;

        // call the callback
        auto next = cb->reportError("Channel is in error state");

        // leap out if channel no longer exists
        if (!monitor.valid()) return;

        // set the oldest callback
        _oldestCallback = next;
    }

    // all callbacks have been processed, so we also can reset the pointer to the newest
    _newestCallback = nullptr;

    // inform handler
    if (notifyhandler && _errorCallback) _errorCallback(message);

    // leap out if object no longer exists
    if (!monitor.valid()) return;

    // the connection no longer has to know that this channel exists,
    // because the channel ID is no longer in use
    if (_connection) _connection->remove(this);
    _connection = nullptr;
}

/**
 *  Get the current receiver for a given consumer tag
 *  @param  consumertag     the consumer frame
 *  @return DeferredConsumer
 */
DeferredConsumer *ChannelImpl::consumer(const std::string &consumertag) const
{
    // look in the map
    auto iter = _consumers.find(consumertag);
    
    // return the result
    return iter == _consumers.end() ? nullptr : iter->second.get();
}

/**
 *  End of namespace
 */
}

