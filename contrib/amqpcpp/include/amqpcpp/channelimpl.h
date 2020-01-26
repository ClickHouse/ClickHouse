/**
 *  ChannelImpl.h
 *
 *  Extended channel object that is used internally by the library, but
 *  that has a private constructor so that it can not be used from outside
 *  the AMQP library
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "exchangetype.h"
#include "watchable.h"
#include "callbacks.h"
#include "copiedbuffer.h"
#include "deferred.h"
#include "monitor.h"
#include <memory>
#include <queue>
#include <map>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class DeferredReceiver;
class BasicDeliverFrame;
class DeferredConsumer;
class BasicGetOKFrame;
class ConsumedMessage;
class ConnectionImpl;
class DeferredDelete;
class DeferredCancel;
class DeferredConfirm;
class DeferredQueue;
class DeferredGet;
class DeferredPublisher;
class Connection;
class Envelope;
class Table;
class Frame;

/**
 *  Class definition
 */
class ChannelImpl : public Watchable, public std::enable_shared_from_this<ChannelImpl>
{
private:
    /**
     *  Pointer to the connection
     *  @var    ConnectionImpl
     */
    ConnectionImpl *_connection = nullptr;

    /**
     *  Callback when the channel is ready
     *  @var    SuccessCallback
     */
    SuccessCallback _readyCallback;

    /**
     *  Callback when the channel errors out
     *  @var    ErrorCallback
     */
    ErrorCallback _errorCallback;

    /**
     *  Handler that deals with incoming messages as a result of publish operations
     *  @var    std::shared_ptr<DeferredPublisher>
     */
    std::shared_ptr<DeferredPublisher> _publisher;

    /**
     * Handler that deals with publisher confirms frames
     * @var    std::shared_ptr<DeferredConfirm>
     */
    std::shared_ptr<DeferredConfirm> _confirm;

    /**
     *  Handlers for all consumers that are active
     *  @var    std::map<std::string,std::shared_ptr<DeferredConsumer>
     */
    std::map<std::string,std::shared_ptr<DeferredConsumer>> _consumers;

    /**
     *  Pointer to the oldest deferred result (the first one that is going
     *  to be executed)
     *
     *  @var    Deferred
     */
    std::shared_ptr<Deferred> _oldestCallback;

    /**
     *  Pointer to the newest deferred result (the last one to be added).
     *
     *  @var    Deferred
     */
    std::shared_ptr<Deferred> _newestCallback;

    /**
     *  The channel number
     *  @var uint16_t
     */
    uint16_t _id = 0;

    /**
     *  State of the channel object
     *  @var enum
     */
    enum {
        state_connected,
        state_ready,
        state_closing,
        state_closed
    } _state = state_closed;

    /**
     *  The frames that still need to be send out
     *
     *  We store the data as well as whether they
     *  should be handled synchronously.
     *
     *  @var std::queue
     */
    std::queue<std::pair<bool, CopiedBuffer>> _queue;

    /**
     *  Are we currently operating in synchronous mode? Meaning: do we first have
     *  to wait for the answer to previous instructions before we send a new instruction?
     *  @var bool
     */
    bool _synchronous = false;

    /**
     *  The current object that is busy receiving a message
     *  @var std::shared_ptr<DeferredReceiver>
     */
    std::shared_ptr<DeferredReceiver> _receiver;

    /**
     *  Attach the connection
     *  @param  connection
     *  @return bool
     */
    bool attach(Connection *connection);

    /**
     *  Push a deferred result
     *  @param  result          The deferred result
     *  @return Deferred        The object just pushed
     */
    Deferred &push(const std::shared_ptr<Deferred> &deferred);

    /**
     *  Send a framen and push a deferred result
     *  @param  frame           The frame to send
     *  @return Deferred        The object just pushed
     */
    Deferred &push(const Frame &frame);

protected:
    /**
     *  Construct a channel object
     *
     *  Note that the constructor is private, and that the Channel class is
     *  a friend. By doing this we ensure that nobody can instantiate this
     *  object, and that it can thus only be used inside the library.
     */
    ChannelImpl();

public:
    /**
     *  Copy'ing of channel objects is not supported
     *  @param  channel
     */
    ChannelImpl(const ChannelImpl &channel) = delete;

    /**
     *  Destructor
     */
    virtual ~ChannelImpl();

    /**
     *  No assignments of other channels
     *  @param  channel
     *  @return Channel
     */
    ChannelImpl &operator=(const ChannelImpl &channel) = delete;

    /**
     *  Invalidate the channel
     *  This method is called when the connection is destructed
     */
    void detach()
    {
        // connection is gone
        _connection = nullptr;
    }

    /**
     *  Callback that is called when the channel was succesfully created.
     *  @param  callback    the callback to execute
     */
    void onReady(const SuccessCallback &callback)
    {
        // store callback
        _readyCallback = callback;

        // direct call if channel is already ready
        if (_state == state_ready && callback) callback();
    }

    /**
     *  Callback that is called when an error occurs.
     *
     *  Only one error callback can be registered. Calling this function
     *  multiple times will remove the old callback.
     *
     *  @param  callback    the callback to execute
     */
    void onError(const ErrorCallback &callback);

    /**
     *  Pause deliveries on a channel
     *
     *  This will stop all incoming messages
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &pause();

    /**
     *  Resume a paused channel
     *
     *  This will resume incoming messages
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &resume();

    /**
     *  Is the channel usable / not yet closed?
     *  @return bool
     */
    bool usable() const
    {
        return _state == state_connected || _state == state_ready;
    }

    /**
     *  Put channel in a confirm mode (RabbitMQ specific)
     */
    DeferredConfirm &confirmSelect();

    /**
     *  Start a transaction
     */
    Deferred &startTransaction();

    /**
     *  Commit the current transaction
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &commitTransaction();

    /**
     *  Rollback the current transaction
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &rollbackTransaction();

    /**
     *  declare an exchange
     *
     *  @param  name        name of the exchange to declare
     *  @param  type        type of exchange
     *  @param  flags       additional settings for the exchange
     *  @param  arguments   additional arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &declareExchange(const std::string &name, ExchangeType type, int flags, const Table &arguments);

    /**
     *  bind two exchanges

     *  @param  source      exchange which binds to target
     *  @param  target      exchange to bind to
     *  @param  routingKey  routing key
     *  @param  arguments   additional arguments for binding
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &bindExchange(const std::string &source, const std::string &target, const std::string &routingkey, const Table &arguments);

    /**
     *  unbind two exchanges

     *  @param  source      the source exchange
     *  @param  target      the target exchange
     *  @param  routingkey  the routing key
     *  @param  arguments   additional unbind arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &unbindExchange(const std::string &source, const std::string &target, const std::string &routingkey, const Table &arguments);

    /**
     *  remove an exchange
     *
     *  @param  name        name of the exchange to remove
     *  @param  flags       additional settings for deleting the exchange
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &removeExchange(const std::string &name, int flags);

    /**
     *  declare a queue
     *  @param  name        queue name
     *  @param  flags       additional settings for the queue
     *  @param  arguments   additional arguments
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    DeferredQueue &declareQueue(const std::string &name, int flags, const Table &arguments);

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
    Deferred &bindQueue(const std::string &exchangeName, const std::string &queueName, const std::string &routingkey, const Table &arguments);

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
    Deferred &unbindQueue(const std::string &exchangeName, const std::string &queueName, const std::string &routingkey, const Table &arguments);

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
    DeferredDelete &purgeQueue(const std::string &name);

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
    DeferredDelete &removeQueue(const std::string &name, int flags);

    /**
     *  Publish a message to an exchange
     *
     *  If the mandatory or immediate flag is set, and the message could not immediately
     *  be published, the message will be returned to the client.
     *
     *  @param  exchange    the exchange to publish to
     *  @param  routingkey  the routing key
     *  @param  envelope    the full envelope to send
     *  @param  message     the message to send
     *  @param  size        size of the message
     *  @param  flags       optional flags
     *  @return DeferredPublisher
     */
    DeferredPublisher &publish(const std::string &exchange, const std::string &routingKey, const Envelope &envelope, int flags);

    /**
     *  Set the Quality of Service (QOS) of the entire connection
     *  @param  prefetchCount       maximum number of messages to prefetch
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     *
     *  @param  count       number of messages to pre-fetch
     *  @param  global      share count between all consumers on the same channel
     */
    Deferred &setQos(uint16_t prefetchCount, bool global = false);

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
    DeferredConsumer& consume(const std::string &queue, const std::string &tag, int flags, const Table &arguments);

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
    DeferredCancel &cancel(const std::string &tag);

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
    DeferredGet &get(const std::string &queue, int flags = 0);

    /**
     *  Acknowledge a message
     *  @param  deliveryTag         the delivery tag
     *  @param  flags               optional flags
     *  @return bool
     */
    bool ack(uint64_t deliveryTag, int flags);

    /**
     *  Reject a message
     *  @param  deliveryTag         the delivery tag
     *  @param  flags               optional flags
     *  @return bool
     */
    bool reject(uint64_t deliveryTag, int flags);
    
    /**
     *  Recover messages that were not yet ack'ed
     *  @param  flags               optional flags
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &recover(int flags);

    /**
     *  Close the current channel
     *
     *  This function returns a deferred handler. Callbacks can be installed
     *  using onSuccess(), onError() and onFinalize() methods.
     */
    Deferred &close();

    /**
     *  Get the channel we're working on
     *  @return uint16_t
     */
    uint16_t id() const
    {
        return _id;
    }

    /**
     *  Send a frame over the channel
     *  @param  frame       frame to send
     *  @return bool        was frame succesfully sent?
     */
    bool send(const Frame &frame);

    /**
     *  Is this channel waiting for an answer before it can send furher instructions
     *  @return bool
     */
    bool waiting() const
    {
        return _synchronous || !_queue.empty();
    }

    /**
     *  Signal the channel that a synchronous operation was completed, and that any
     *  queued frames can be sent out.
     */
    void flush();

    /**
     *  Report to the handler that the channel is opened
     */
    void reportReady()
    {
        // callbacks could destroy us, so monitor it
        Monitor monitor(this);

        // if we are still in connected state we are now ready
        if (_state == state_connected) _state = state_ready;
        
        // the last (possibly synchronous) operation was received, so we're no longer in synchronous mode
        if (_synchronous && _queue.empty()) _synchronous = false;

        // inform handler
        if (_readyCallback) _readyCallback();

        // if the monitor is still valid, we flush any waiting operations 
        if (monitor.valid()) flush();
    }

    /**
     *  Report to the handler that the channel is closed
     *
     *  Returns whether the channel object is still valid
     *
     *  @return bool
     */
    bool reportClosed()
    {
        // change state
        _state = state_closed;

        // create a monitor, because the callbacks could destruct the current object
        Monitor monitor(this);

        // and pass on to the reportSuccess() method which will call the
        // appropriate deferred object to report the successful operation
        bool result = reportSuccess();

        // leap out if object no longer exists
        if (!monitor.valid()) return result;

        // all later deferred objects should report an error, because it
        // was not possible to complete the instruction as the channel is
        // now closed (but the channel onError does not have to run)
        reportError("Channel has been closed", false);

        // done
        return result;
    }

    /**
     *  Report success
     *
     *  Returns whether the channel object is still valid
     *
     *  @param  mixed
     *  @return bool
     */
    template <typename... Arguments>
    bool reportSuccess(Arguments ...parameters)
    {
        // skip if there is no oldest callback
        if (!_oldestCallback) return true;

        // flush the queue, which will send the next operation if the current operation was synchronous
        flush();
        
        // we are going to call callbacks that could destruct the channel
        Monitor monitor(this);

        // copy the callback (so that it will not be destructed during
        // the "reportSuccess" call, if the channel is destructed during the call)
        auto cb = _oldestCallback;

        // call the callback
        auto next = cb->reportSuccess(std::forward<Arguments>(parameters)...);

        // leap out if channel no longer exists
        if (!monitor.valid()) return false;

        // set the oldest callback
        _oldestCallback = next;

        // if there was no next callback, the newest callback was just used
        if (!next) _newestCallback = nullptr;

        // we are still valid
        return true;
    }

    /**
     *  Report an error message on a channel
     *  @param  message             the error message
     *  @param  notifyhandler       should the channel-wide handler also be called?
     */
    void reportError(const char *message, bool notifyhandler = true);

    /**
     *  Install a consumer
     *  @param  consumertag     The consumer tag
     *  @param  consumer        The consumer object
     */
    void install(const std::string &consumertag, const std::shared_ptr<DeferredConsumer> &consumer)
    {
        // install the consumer handler
        _consumers[consumertag] = consumer;
    }

    /**
     *  Install the current consumer
     *  @param  receiver        The receiver object
     */
    void install(const std::shared_ptr<DeferredReceiver> &receiver)
    {
        // store object as current receiver
        _receiver = receiver;
    }

    /**
     *  Uninstall a consumer callback
     *  @param  consumertag     The consumer tag
     */
    void uninstall(const std::string &consumertag)
    {
        // erase the callback
        _consumers.erase(consumertag);
    }

    /**
     *  Fetch the receiver for a specific consumer tag
     *  @param  consumertag the consumer tag
     *  @return             the receiver object
     */
    DeferredConsumer *consumer(const std::string &consumertag) const;

    /**
     *  Retrieve the current object that is receiving a message
     *  @return The handler responsible for the current message
     */
    DeferredReceiver *receiver() const { return _receiver.get(); }
    
    /**
     *  Retrieve the deferred publisher that handles returned messages
     *  @return The deferred publisher object
     */
    DeferredPublisher *publisher() const { return _publisher.get(); }

    /**
     * Retrieve the deferred confirm that handles publisher confirms
     * @return The deferred confirm object
     */
    DeferredConfirm *confirm() const { return _confirm.get(); }

    /**
     *  The channel class is its friend, thus can it instantiate this object
     */
    friend class Channel;
};

/**
 *  End of namespace
 */
}

