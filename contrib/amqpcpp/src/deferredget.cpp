/**
 *  DeferredGet.cpp
 *
 *  Implementation of the DeferredGet call
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "includes.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Report success for a get operation
 *  @param  messagecount    Number of messages left in the queue
 *  @param  deliveryTag     Delivery tag of the message coming in
 *  @param  redelivered     Was the message redelivered?
 */
const std::shared_ptr<Deferred> &DeferredGet::reportSuccess(uint32_t messagecount, uint64_t deliveryTag, bool redelivered)
{
    // install this object as the handler for the upcoming header and body frames
    _channel->install(shared_from_this());
    
    // store delivery tag and redelivery status
    _deliveryTag = deliveryTag;
    _redelivered = redelivered;

    // report the size (note that this is the size _minus_ the message that is retrieved
    // (and for which the callback will be called later), so it could be zero)
    if (_countCallback) _countCallback(messagecount);

    // return next handler
    return _next;
}

/**
 *  Report success, although no message could be gotten
 *  @return Deferred
 */
const std::shared_ptr<Deferred> &DeferredGet::reportSuccess() const
{
    // report the size
    if (_countCallback) _countCallback(0);

    // check if a callback was set
    if (_emptyCallback) _emptyCallback();

    // return next object
    return _next;
}

/**
 *  Extended implementation of the complete method that is called when a message was fully received
 */
void DeferredGet::complete()
{
    // the channel is now synchronized, delayed frames may now be sent
    _channel->flush();
    
    // pass on to normal implementation
    DeferredExtReceiver::complete();
}

/**
 *  End of namespace
 */
}

