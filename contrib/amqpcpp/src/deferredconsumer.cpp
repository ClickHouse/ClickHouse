/**
 *  DeferredConsumer.cpp
 *
 *  Implementation file for the DeferredConsumer class
 *
 *  @copyright 2014 - 2018 Copernica BV
 */
#include "includes.h"
#include "basicdeliverframe.h"

/**
 *  Namespace
 */
namespace AMQP {

/**
 *  Process a delivery frame
 *
 *  @param  frame   The frame to process
 */
void DeferredConsumer::process(BasicDeliverFrame &frame)
{
    // this object will handle all future frames with header and body data
    _channel->install(shared_from_this());
    
    // retrieve the delivery tag and whether we were redelivered
    _deliveryTag = frame.deliveryTag();
    _redelivered = frame.redelivered();

    // initialize the object for the next message
    initialize(frame.exchange(), frame.routingKey());
}

/**
 *  Report success for frames that report start consumer operations
 *  @param  name            Consumer tag that is started
 *  @return Deferred
 */
const std::shared_ptr<Deferred> &DeferredConsumer::reportSuccess(const std::string &name)
{
    // we now know the name, so install ourselves in the channel
    _channel->install(name, shared_from_this());

    // skip if no special callback was installed
    if (!_consumeCallback) return Deferred::reportSuccess();

    // call the callback
    _consumeCallback(name);

    // return next object
    return _next;
}

/**
 *  End namespace
 */
}
