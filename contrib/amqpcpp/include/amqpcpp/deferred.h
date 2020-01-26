/**
 *  Deferred.h
 *
 *  Class describing a set of actions that could
 *  possibly happen in the future that can be
 *  caught.
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
#include <memory>
#include <stdint.h>
#include "callbacks.h"

/**
 *  Set up namespace
 */
namespace AMQP {

// forward declaration
class ChannelImpl;

/**
 *  Class definition
 */
class Deferred
{
private:
    /**
     *  Callback to execute either way
     *  @var    FinalizeCallback
     */
    FinalizeCallback _finalizeCallback;

protected:
    /**
     *  Callback to execute on success
     *  @var    SuccessCallback
     */
    SuccessCallback _successCallback;

    /**
     *  Callback to execute on failure
     *  @var    ErrorCallback
     */
    ErrorCallback _errorCallback;

    /**
     *  Pointer to the next deferred object
     *  @var    Deferred
     */
    std::shared_ptr<Deferred> _next;

    /**
     *  Do we already know we failed?
     *  @var bool
     */
    bool _failed;


    /**
     *  The next deferred object
     *  @return Deferred
     */
    const std::shared_ptr<Deferred> &next() const
    {
        return _next;
    }

    /**
     *  Indicate success
     *  @return Deferred        Next deferred result
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess() const
    {
        // execute callbacks if registered
        if (_successCallback) _successCallback();

        // return the next deferred result
        return _next;
    }

    /**
     *  Report success for queue declared messages
     *  @param  name            Name of the new queue
     *  @param  messagecount    Number of messages in the queue
     *  @param  consumercount   Number of consumers linked to the queue
     *  @return Deferred        Next deferred result
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(const std::string &name, uint32_t messagecount, uint32_t consumercount) const
    {
        // make sure compilers dont complain about unused parameters
        (void) name;
        (void) messagecount;
        (void) consumercount;

        // this is the same as a regular success message
        return reportSuccess();
    }

    /**
     *  Report success for frames that report delete operations
     *  @param  messagecount    Number of messages that were deleted
     *  @return Deferred
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(uint32_t messagecount) const
    {
        // make sure compilers dont complain about unused parameters
        (void) messagecount;

        // this is the same as a regular success message
        return reportSuccess();
    }

    /**
     *  Report success for a get operation
     *
     *  @param  messagecount    Number of messages left in the queue
     *  @param  deliveryTag     Delivery tag of the message coming in
     *  @param  redelivered     Was the message redelivered?
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(uint32_t messagecount, uint64_t deliveryTag, bool redelivered)
    {
        // make sure compilers dont complain about unused parameters
        (void) messagecount;
        (void) deliveryTag;
        (void) redelivered;

        // this is the same as a regular success message
        return reportSuccess();
    }

    /**
     *  Report success for frames that report cancel operations
     *  @param  name            Consumer tag that is cancelled
     *  @return Deferred
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(const std::string &name)
    {
        // make sure compilers dont complain about unused parameters
        (void) name;

        // this is the same as a regular success message
        return reportSuccess();
    }

    /**
     *  Indicate failure
     *  @param  error           Description of the error that occured
     *  @return Deferred        Next deferred result
     */
    const std::shared_ptr<Deferred> &reportError(const char *error)
    {
        // from this moment on the object should be listed as failed
        _failed = true;

        // execute callbacks if registered
        if (_errorCallback) _errorCallback(error);

        // return the next deferred result
        return _next;
    }

    /**
     *  Add a pointer to the next deferred result
     *  @param  deferred
     */
    void add(const std::shared_ptr<Deferred> &deferred)
    {
        // store pointer
        _next = deferred;
    }

    /**
     *  The channel implementation may call our
     *  private members and construct us
     */
    friend class ChannelImpl;

public:
    /**
     *  Protected constructor that can only be called
     *  from within the channel implementation
     *
     *  Note: this constructor _should_ be protected, but because make_shared
     *  will then not work, we have decided to make it public after all,
     *  because the work-around would result in not-so-easy-to-read code.
     *
     *  @param  failed  are we already failed?
     */
    Deferred(bool failed = false) : _failed(failed) {}

public:
    /**
     *  Deleted copy and move constructors
     */
    Deferred(const Deferred &that) = delete;
    Deferred(Deferred &&that) = delete;

    /**
     *  Destructor
     */
    virtual ~Deferred()
    {
        // report to the finalize callback
        if (_finalizeCallback) _finalizeCallback();
    }

    /**
     *  Cast to a boolean
     */
    operator bool () const
    {
        return !_failed;
    }

    /**
     *  Register a function to be called
     *  if and when the operation succesfully
     *  completes.
     *
     *  Only one callback can be registered at a time.
     *  Successive calls to this function will clear
     *  callbacks registered before.
     *
     *  @param  callback    the callback to execute
     */
    Deferred &onSuccess(const SuccessCallback &callback)
    {
        // store callback
        _successCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Register a function to be called
     *  if and when the operation fails.
     *
     *  Only one callback can be registered at a time.
     *  Successive calls to this function will clear
     *  callbacks registered before.
     *
     *  @param  callback    the callback to execute
     */
    Deferred &onError(const ErrorCallback &callback)
    {
        // store callback
        _errorCallback = callback;

        // if the object is already in a failed state, we call the callback right away
        if (_failed) callback("Frame could not be sent");

        // allow chaining
        return *this;
    }

    /**
     *  Register a function to be called
     *  if and when the operation completes
     *  or fails. This function will be called
     *  either way.
     *
     *  In the case of success, the provided
     *  error parameter will be an empty string.
     *
     *  Only one callback can be registered at at time.
     *  Successive calls to this function will clear
     *  callbacks registered before.
     *
     *  @param  callback    the callback to execute
     */
    Deferred &onFinalize(const FinalizeCallback &callback)
    {
        // store callback
        _finalizeCallback = callback;

        // if the object is already in a failed state, we call the callback right away
        if (_failed) callback();

        // allow chaining
        return *this;
    }
};

/**
 *  End namespace
 */
}
