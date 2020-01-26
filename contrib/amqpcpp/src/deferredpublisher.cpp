/**
 *  DeferredPublisher.cpp
 *
 *  Implementation file for the DeferredPublisher class
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */
#include "includes.h"
#include "basicreturnframe.h"

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Process a return frame
 *
 *  @param  frame   The frame to process
 */
void DeferredPublisher::process(BasicReturnFrame &frame)
{
    // this object will handle all future frames with header and body data
    _channel->install(shared_from_this());

    // retrieve the delivery tag and whether we were redelivered
    _code = frame.replyCode();
    _description = frame.replyText();
    
    // notify user space of the begin of the returned message
    if (_beginCallback) _beginCallback(_code, _description);

    // initialize the object for the next message
    initialize(frame.exchange(), frame.routingKey());

    // do we have anybody interested in messages? in that case we construct the message
    if (_bounceCallback) _message.construct(frame.exchange(), frame.routingKey());
}

/**
 *  Indicate that a message was done
 */
void DeferredPublisher::complete()
{
    // also monitor the channel
    Monitor monitor(_channel);

    // do we have a message?
    if (_message) _bounceCallback(*_message, _code, _description);

    // do we have to inform anyone about completion?
    if (_completeCallback) _completeCallback();
    
    // for the next iteration we want a new message
    _message.reset();
    
    // the description can be thrown away too
    _description.clear();

    // do we still have a valid channel
    if (!monitor.valid()) return;

    // we are now done executing, so the channel can forget the current receiving object
    _channel->install(nullptr);
}

/**
 *  End of namespace
 */
}

