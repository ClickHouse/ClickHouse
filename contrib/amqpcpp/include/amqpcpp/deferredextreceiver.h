/**
 *  DeferredExtReceiver.h
 *
 *  Extended receiver that _wants_ to receive message (because it is
 *  consuming or get'ting messages. This is the base class for both
 *  the DeferredConsumer as well as the DeferredGet classes, but not
 *  the base of the DeferredPublisher (which can also receive returned
 *  messages, but not as a result of an explicit request)
 *  
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "deferredreceiver.h"

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class DeferredExtReceiver : public DeferredReceiver
{
protected:
    /**
     *  The delivery tag for the current message
     *  @var    uint64_t
     */
    uint64_t _deliveryTag = 0;

    /**
     *  Is this a redelivered message
     *  @var    bool
     */
    bool _redelivered = false;

    /**
     *  Callback for incoming messages
     *  @var    MessageCallback
     */
    MessageCallback _messageCallback;

    /**
     *  Callback for when a message was complete finished
     *  @var    DeliveredCallback
     */
    DeliveredCallback _deliveredCallback;


    /**
     *  Initialize the object to send out a message
     *  @param  exchange            the exchange to which the message was published
     *  @param  routingkey          the routing key that was used to publish the message
     */
    virtual void initialize(const std::string &exchange, const std::string &routingkey) override;

    /**
     *  Indicate that a message was done
     */
    virtual void complete() override;

    /**
     *  Constructor
     *  @param  failed  Have we already failed?
     *  @param  channel The channel we are consuming on
     */
    DeferredExtReceiver(bool failed, ChannelImpl *channel) : 
        DeferredReceiver(failed, channel) {}
    
public:
    /**
     *  Destructor
     */
    virtual ~DeferredExtReceiver() = default;
};
    
/**
 *  End of namespace
 */
}

