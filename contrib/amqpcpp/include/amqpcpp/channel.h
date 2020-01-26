/**
 *  Class describing a (mid-level) AMQP channel implementation
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class Channel
{
private:
    /**
     *  The implementation for the channel
     *  @var    std::unique_ptr<ChannelImpl>
     */
    std::shared_ptr<ChannelImpl> _implementation;

public:
    /**
     *  Construct a channel object
     * 
     *  The passed in connection pointer must remain valid for the 
     *  lifetime of the channel. Watch out: this method throws an error
     *  if the channel could not be constructed (for example because the
     *  max number of AMQP channels has been reached)
     * 
     *  @param  connection
     *  @throws std::runtime_error
     */
    Channel(Connection *connection);
    
    /**
     *  Copy'ing of channel objects is not supported
     *  @param  channel
     */
    Channel(const Channel &channel) = delete;

    /**
     *  But movement _is_ allowed
     *  @param  channel
     */
    Channel(Channel &&channel) : _implementation(std::move(channel._implementation)) {}

    /**
     *  Destructor
     */
    virtual ~Channel() 
    {
        // close the channel (this will eventually destruct the channel)
        _implementation->close();
    }

    /**
     *  No assignments of other channels
     *  @param  channel
     *  @return Channel
     */
    Channel &operator=(const Channel &channel) = delete;

    /**
     *  Callback that is called when the channel was succesfully created.
     *
     *  Only one callback can be registered. Calling this function multiple
     *  times will remove the old callback.
     *
     *  @param  callback    the callback to execute
     */
    void onReady(const SuccessCallback &callback)
    {
        _implementation->onReady(callback);
    }

    /**
     *  Callback that is called when an error occurs.
     *
     *  Only one error callback can be registered. Calling this function
     *  multiple times will remove the old callback.
     *
     *  @param  callback    the callback to execute
     */
    void onError(const ErrorCallback &callback)
    {
        _implementation->onError(callback);
    }

    /**
     *  Pause deliveries on a channel
     *
     *  This will stop all incoming messages
     *
     *  Note that this function does *not* work using RabbitMQ. For more info
     *  @see https://www.rabbitmq.com/specification.html#method-status-channel.flow
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &pause()
    {
        return _implementation->pause();
    }

    /**
     *  Resume a paused channel
     *
     *  This will resume incoming messages
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &resume()
    {
        return _implementation->resume();
    }

    /**
     *  Is the channel usable / not yet closed?
     *  @return bool
     */
    bool usable() const
    {
        return _implementation->usable();
    }
    
    /**
     *  Is the channel connected?
     *  This method is deprecated: use Channel::usable()
     *  @return bool
     *  @deprecated
     */
    bool connected() const
    {
        return usable();
    }

    /**
     *  Put channel in a confirm mode (RabbitMQ specific)
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    DeferredConfirm &confirmSelect()
    {
        return _implementation->confirmSelect();
    }

    /**
     *  Start a transaction
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &startTransaction()
    {
        return _implementation->startTransaction();
    }

    /**
     *  Commit the current transaction
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &commitTransaction()
    {
        return _implementation->commitTransaction();
    }

    /**
     *  Rollback the current transaction
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &rollbackTransaction()
    {
        return _implementation->rollbackTransaction();
    }

    /**
     *  Declare an exchange
     *
     *  If an empty name is supplied, a name will be assigned by the server.
     *
     *  The following flags can be used for the exchange:
     *
     *      -   durable     exchange survives a broker restart
     *      -   autodelete  exchange is automatically removed when all connected queues are removed
     *      -   passive     only check if the exchange exist
     *      -   internal    create an internal exchange
     *
     *  @param  name        name of the exchange
     *  @param  type        exchange type
     *  @param  flags       exchange flags
     *  @param  arguments   additional arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &declareExchange(const std::string &name, ExchangeType type, int flags, const Table &arguments) { return _implementation->declareExchange(name, type, flags, arguments); }
    Deferred &declareExchange(const std::string &name, ExchangeType type, const Table &arguments) { return _implementation->declareExchange(name, type, 0, arguments); }
    Deferred &declareExchange(const std::string &name, ExchangeType type = fanout, int flags = 0) { return _implementation->declareExchange(name, type, flags, Table()); }
    Deferred &declareExchange(ExchangeType type, int flags, const Table &arguments) { return _implementation->declareExchange(std::string(), type, flags, arguments); }
    Deferred &declareExchange(ExchangeType type, const Table &arguments) { return _implementation->declareExchange(std::string(), type, 0, arguments); }
    Deferred &declareExchange(ExchangeType type = fanout, int flags = 0) { return _implementation->declareExchange(std::string(), type, flags, Table()); }

    /**
     *  Remove an exchange
     *
     *  The following flags can be used for the exchange:
     *
     *      -   ifunused    only delete if no queues are connected

     *  @param  name        name of the exchange to remove
     *  @param  flags       optional flags
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &removeExchange(const std::string &name, int flags = 0) { return _implementation->removeExchange(name, flags); }

    /**
     *  Bind two exchanges to each other
     *
     *  @param  source      the source exchange
     *  @param  target      the target exchange
     *  @param  routingkey  the routing key
     *  @param  arguments   additional bind arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &bindExchange(const std::string &source, const std::string &target, const std::string &routingkey, const Table &arguments) { return _implementation->bindExchange(source, target, routingkey, arguments); }
    Deferred &bindExchange(const std::string &source, const std::string &target, const std::string &routingkey) { return _implementation->bindExchange(source, target, routingkey, Table()); }

    /**
     *  Unbind two exchanges from one another
     *
     *  @param  target      the target exchange
     *  @param  source      the source exchange
     *  @param  routingkey  the routing key
     *  @param  arguments   additional unbind arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &unbindExchange(const std::string &target, const std::string &source, const std::string &routingkey, const Table &arguments) { return _implementation->unbindExchange(target, source, routingkey, arguments); }
    Deferred &unbindExchange(const std::string &target, const std::string &source, const std::string &routingkey) { return _implementation->unbindExchange(target, source, routingkey, Table()); }

    /**
     *  Declare a queue
     *
     *  If you do not supply a name, a name will be assigned by the server.
     *
     *  The flags can be a combination of the following values:
     *
     *      -   durable     queue survives a broker restart
     *      -   autodelete  queue is automatically removed when all connected consumers are gone
     *      -   passive     only check if the queue exist
     *      -   exclusive   the queue only exists for this connection, and is automatically removed when connection is gone
     *
     *  @param  name        name of the queue
     *  @param  flags       combination of flags
     *  @param  arguments   optional arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     *
     *  The onSuccess() callback that you can install should have the following signature:
     *
     *      void myCallback(const std::string &name, uint32_t messageCount, uint32_t consumerCount);
     *
     *  For example: channel.declareQueue("myqueue").onSuccess([](const std::string &name, uint32_t messageCount, uint32_t consumerCount) {
     *
     *      std::cout << "Queue '" << name << "' has been declared with " << messageCount << " messages and " << consumerCount << " consumers" << std::endl;
     *
     *  });
     */
    DeferredQueue &declareQueue(const std::string &name, int flags, const Table &arguments) { return _implementation->declareQueue(name, flags, arguments); }
    DeferredQueue &declareQueue(const std::string &name, const Table &arguments) { return _implementation->declareQueue(name, 0, arguments); }
    DeferredQueue &declareQueue(const std::string &name, int flags = 0) { return _implementation->declareQueue(name, flags, Table()); }
    DeferredQueue &declareQueue(int flags, const Table &arguments) { return _implementation->declareQueue(std::string(), flags, arguments); }
    DeferredQueue &declareQueue(const Table &arguments) { return _implementation->declareQueue(std::string(), 0, arguments); }
    DeferredQueue &declareQueue(int flags = 0) { return _implementation->declareQueue(std::string(), flags, Table()); }

    /**
     *  Bind a queue to an exchange
     *
     *  @param  exchange    the source exchange
     *  @param  queue       the target queue
     *  @param  routingkey  the routing key
     *  @param  arguments   additional bind arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &bindQueue(const std::string &exchange, const std::string &queue, const std::string &routingkey, const Table &arguments) { return _implementation->bindQueue(exchange, queue, routingkey, arguments); }
    Deferred &bindQueue(const std::string &exchange, const std::string &queue, const std::string &routingkey) { return _implementation->bindQueue(exchange, queue, routingkey, Table()); }

    /**
     *  Unbind a queue from an exchange
     *  @param  exchange    the source exchange
     *  @param  queue       the target queue
     *  @param  routingkey  the routing key
     *  @param  arguments   additional bind arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &unbindQueue(const std::string &exchange, const std::string &queue, const std::string &routingkey, const Table &arguments) {  return _implementation->unbindQueue(exchange, queue, routingkey, arguments); }
    Deferred &unbindQueue(const std::string &exchange, const std::string &queue, const std::string &routingkey) { return _implementation->unbindQueue(exchange, queue, routingkey, Table()); }

    /**
     *  Purge a queue
     *
     *  @param  name        name of the queue
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     *
     *  The onSuccess() callback that you can install should have the following signature:
     *
     *      void myCallback(uint32_t messageCount);
     *
     *  For example: channel.declareQueue("myqueue").onSuccess([](uint32_t messageCount) {
     *
     *      std::cout << "Queue purged, all " << messageCount << " messages removed" << std::endl;
     *
     *  });
     */
    DeferredDelete &purgeQueue(const std::string &name){ return _implementation->purgeQueue(name); }

    /**
     *  Remove a queue
     *
     *  The following flags can be used for the exchange:
     *
     *      -   ifunused    only delete if no consumers are connected
     *      -   ifempty     only delete if the queue is empty
     *
     *  @param  name        name of the queue to remove
     *  @param  flags       optional flags
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     *
     *  The onSuccess() callback that you can install should have the following signature:
     *
     *      void myCallback(uint32_t messageCount);
     *
     *  For example: channel.removeQueue("myqueue").onSuccess([](uint32_t messageCount) {
     *
     *      std::cout << "Queue deleted, along with " << messageCount << " messages" << std::endl;
     *
     *  });
     */
    DeferredDelete &removeQueue(const std::string &name, int flags = 0) { return _implementation->removeQueue(name, flags); }

    /**
     *  Publish a message to an exchange
     * 
     *  You have to supply the name of an exchange and a routing key. RabbitMQ will then try
     *  to send the message to one or more queues. With the optional flags parameter you can
     *  specify what should happen if the message could not be routed to a queue. By default,
     *  unroutable message are silently discarded.
     * 
     *  This method returns a reference to a DeferredPublisher object. You can use this returned
     *  object to install callbacks that are called when an undeliverable message is returned, or 
     *  to set the callback that is called when the server confirms that the message was received. 
     * 
     *  To enable handling returned messages, or to enable publisher-confirms, you must not only
     *  set the callback, but also pass in appropriate flags to enable this feature. If you do not 
     *  pass in these flags, your callbacks will not be called. If you are not at all interested
     *  in returned messages or publish-confirms, you can ignore the flag and the returned object.
     * 
     *  Watch out: the channel returns _the same_ DeferredPublisher object for all calls to the 
     *  publish() method. This means that the callbacks that you install for the first published 
     *  message are also used for subsequent messages _and_ it means that if you install a different 
     *  callback for a later publish operation, it overwrites your earlier callbacks 
     * 
     *  The following flags can be supplied:
     * 
     *      -   mandatory   If set, server returns messages that are not sent to a queue
     *      -   immediate   If set, server returns messages that can not immediately be forwarded to a consumer. 
     * 
     *  @param  exchange    the exchange to publish to
     *  @param  routingkey  the routing key
     *  @param  envelope    the full envelope to send
     *  @param  message     the message to send
     *  @param  size        size of the message
     *  @param  flags       optional flags
     */
    DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const Envelope &envelope, int flags = 0) { return _implementation->publish(exchange, routingKey, envelope, flags); }
    DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const std::string &message, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message.data(), message.size()), flags); }
    DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const char *message, size_t size, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message, size), flags); }
    DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const char *message, int flags = 0) { return _implementation->publish(exchange, routingKey, Envelope(message, strlen(message)), flags); }

    /**
     *  Set the Quality of Service (QOS) for this channel
     *
     *  When you consume messages, every single message needs to be ack'ed to inform
     *  the RabbitMQ server that is has been received. The Qos setting specifies the
     *  number of unacked messages that may exist in the client application. The server
     *  stops delivering more messages if the number of unack'ed messages has reached
     *  the prefetchCount
     *
     *  @param  prefetchCount       maximum number of messages to prefetch
     *  @param  global              share counter between all consumers on the same channel
     *  @return bool                whether the Qos frame is sent.
     */
    Deferred &setQos(uint16_t prefetchCount, bool global = false)
    {
        return _implementation->setQos(prefetchCount, global);
    }

    /**
     *  Tell the RabbitMQ server that we're ready to consume messages
     *
     *  After this method is called, RabbitMQ starts delivering messages to the client
     *  application. The consume tag is a string identifier that you can use to identify
     *  the consumer if you later want to stop it with with a channel::cancel() call. 
     *  If you do not specify a consumer tag, the server will assign one for you.
     *
     *  The following flags are supported:
     *
     *      -   nolocal             if set, messages published on this channel are not also consumed
     *      -   noack               if set, consumed messages do not have to be acked, this happens automatically
     *      -   exclusive           request exclusive access, only this consumer can access the queue
     *
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
     *      void myCallback(const std::string& tag);
     *
     *  For example: channel.consume("myqueue").onSuccess([](const std::string& tag) {
     *
     *      std::cout << "Started consuming under tag " << tag << std::endl;
     *
     *  });
     */
    DeferredConsumer &consume(const std::string &queue, const std::string &tag, int flags, const Table &arguments) { return _implementation->consume(queue, tag, flags, arguments); }
    DeferredConsumer &consume(const std::string &queue, const std::string &tag, int flags = 0) { return _implementation->consume(queue, tag, flags, Table()); }
    DeferredConsumer &consume(const std::string &queue, const std::string &tag, const Table &arguments) { return _implementation->consume(queue, tag, 0, arguments); }
    DeferredConsumer &consume(const std::string &queue, int flags, const Table &arguments) { return _implementation->consume(queue, std::string(), flags, arguments); }
    DeferredConsumer &consume(const std::string &queue, int flags = 0) { return _implementation->consume(queue, std::string(), flags, Table()); }
    DeferredConsumer &consume(const std::string &queue, const Table &arguments) { return _implementation->consume(queue, std::string(), 0, arguments); }

    /**
     *  Cancel a running consume call
     *
     *  If you want to stop a running consumer, you can use this method with the consumer tag
     *
     *  @param  tag                 the consumer tag
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     *
     *  The onSuccess() callback that you can install should have the following signature:
     *
     *      void myCallback(const std::string& tag);
     *
     *  For example: channel.cancel("myqueue").onSuccess([](const std::string& tag) {
     *
     *      std::cout << "Stopped consuming under tag " << tag << std::endl;
     *
     *  });
     */
    DeferredCancel &cancel(const std::string &tag) { return _implementation->cancel(tag); }

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
    DeferredGet &get(const std::string &queue, int flags = 0) { return _implementation->get(queue, flags); }

    /**
     *  Acknoldge a received message
     *
     *  When a message is received in the DeferredConsumer::onReceived() method,
     *  you must acknowledge it so that RabbitMQ removes it from the queue (unless
     *  you are consuming with the noack option). This method can be used for
     *  this acknowledging.
     *
     *  The following flags are supported:
     *
     *      -   multiple            acknowledge multiple messages: all un-acked messages that were earlier delivered are acknowledged too
     *
     *  @param  deliveryTag         the unique delivery tag of the message
     *  @param  flags               optional flags
     *  @return bool
     */
    bool ack(uint64_t deliveryTag, int flags=0) { return _implementation->ack(deliveryTag, flags); }

    /**
     *  Reject or nack a message
     *
     *  When a message was received in the DeferredConsumer::onReceived() method,
     *  and you don't want to acknowledge it, you can also choose to reject it by
     *  calling this reject method.
     *
     *  The following flags are supported:
     *
     *      -   multiple            reject multiple messages: all un-acked messages that were earlier delivered are unacked too
     *      -   requeue             if set, the message is put back in the queue, otherwise it is dead-lettered/removed
     *
     *  @param  deliveryTag         the unique delivery tag of the message
     *  @param  flags               optional flags
     *  @return bool
     */
    bool reject(uint64_t deliveryTag, int flags=0) { return _implementation->reject(deliveryTag, flags); }

    /**
     *  Recover all messages that were not yet acked
     *
     *  This method asks the server to redeliver all unacknowledged messages on a specified
     *  channel. Zero or more messages may be redelivered.
     *
     *  The following flags are supported:
     *
     *      -   requeue             if set, the server will requeue the messages, so the could also end up with at different consumer
     *
     *  @param  flags
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &recover(int flags = 0) { return _implementation->recover(flags); }

    /**
     *  Close the current channel
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &close() { return _implementation->close(); }

    /**
     *  Get the channel we're working on
     *  @return uint16_t
     */
    uint16_t id() const
    {
        return _implementation->id();
    }
};

/**
 *  end namespace
 */
}

