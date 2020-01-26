/**
 *  DeferredCancel.cpp
 *
 *  Implementation file for the DeferredCancel class
 *
 *  @copyright 2014 Copernica BV
 */
#include "includes.h"

/**
 *  Namespace
 */
namespace AMQP {

/**
 *  Report success for frames that report cancel operations
 *  @param  name            Consumer tag that is cancelled
 *  @return Deferred
 */
const std::shared_ptr<Deferred> &DeferredCancel::reportSuccess(const std::string &name)
{
    // in the channel, we should uninstall the consumer
    _channel->uninstall(name);

    // skip if no special callback was installed
    if (!_cancelCallback) return Deferred::reportSuccess();

    // call the callback
    _cancelCallback(name);

    // return next object
    return _next;
}

/**
 *  End namespace
 */
}

