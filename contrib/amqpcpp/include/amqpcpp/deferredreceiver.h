/**
 *  DeferredReceiver.h
 *
 *  Base class for the deferred consumer, the deferred get and the
 *  deferred publisher (that may receive returned messages)
 *
 *  @copyright 2016 - 2018 Copernica B.V.
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "deferred.h"
#include "stack_ptr.h"
#include "message.h"

/**
 *  Start namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class BasicDeliverFrame;
class BasicGetOKFrame;
class BasicHeaderFrame;
class BodyFrame;

/**
 *  Base class for deferred consumers
 */
class DeferredReceiver : public Deferred
{
private:
    /**
     *  Size of the body of the current message
     *  @var    uint64_t
     */
    uint64_t _bodySize = 0;


protected:
    /**
     *  Initialize the object to send out a message
     *  @param  exchange            the exchange to which the message was published
     *  @param  routingkey          the routing key that was used to publish the message
     */
    virtual void initialize(const std::string &exchange, const std::string &routingkey);
    
    /**
     *  Get reference to self to prevent that object falls out of scope
     *  @return std::shared_ptr
     */
    virtual std::shared_ptr<DeferredReceiver> lock() = 0;
    
    /**
     *  Indicate that a message was done
     */
    virtual void complete() = 0;

private:
    /**
     *  Process the message headers
     *
     *  @param  frame   The frame to process
     */
    void process(BasicHeaderFrame &frame);

    /**
     *  Process the message data
     *
     *  @param  frame   The frame to process
     */
    void process(BodyFrame &frame);

    /**
     *  Frames may be processed
     */
    friend class ChannelImpl;
    friend class BasicGetOKFrame;
    friend class BasicHeaderFrame;
    friend class BodyFrame;

protected:
    /**
     *  The channel to which the consumer is linked
     *  @var    ChannelImpl
     */
    ChannelImpl *_channel;

    /**
     *  Callback for new message
     *  @var    StartCallback
     */
    StartCallback _startCallback;

    /**
     *  Callback that is called when size of the message is known
     *  @var    SizeCallback
     */
    SizeCallback _sizeCallback;

    /**
     *  Callback for incoming headers
     *  @var    HeaderCallback
     */
    HeaderCallback _headerCallback;

    /**
     *  Callback for when a chunk of data comes in
     *  @var    DataCallback
     */
    DataCallback _dataCallback;

    /**
     *  The message that we are currently receiving
     *  @var    stack_ptr<Message>
     */
    stack_ptr<Message> _message;

    /**
     *  Constructor
     *  @param  failed  Have we already failed?
     *  @param  channel The channel we are consuming on
     */
    DeferredReceiver(bool failed, ChannelImpl *channel) : 
        Deferred(failed), _channel(channel) {}

public:
    /**
     *  Destructor
     */
    virtual ~DeferredReceiver() = default;
};

/**
 *  End namespace
 */
}
