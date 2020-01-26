/**
 *  DeferredDelete.h
 *
 *  Deferred callback for instructions that delete or purge queues, and that
 *  want to report the number of deleted messages.
 *
 *  @copyright 2014 Copernica BV
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
 *  We extend from the default deferred and add extra functionality
 */
class DeferredDelete : public Deferred
{
private:
    /**
     *  Callback to execute when the instruction is completed
     *  @var    DeleteCallback
     */
    DeleteCallback _deleteCallback;

    /**
     *  Report success for queue delete and queue purge messages
     *  @param  messagecount    Number of messages that were deleted
     *  @return Deferred        Next deferred result
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(uint32_t messagecount) const override
    {
        // skip if no special callback was installed
        if (!_deleteCallback) return Deferred::reportSuccess();
        
        // call the callback
        _deleteCallback(messagecount);
        
        // return next object
        return _next;
    }


    /**
     *  The channel implementation may call our
     *  private members and construct us
     */
    friend class ChannelImpl;
    friend class ConsumedMessage;
    
public:
    /**
     *  Protected constructor that can only be called
     *  from within the channel implementation
     * 
     *  Note: this constructor _should_ be protected, but because make_shared
     *  will then not work, we have decided to make it public after all,
     *  because the work-around would result in not-so-easy-to-read code.
     *
     *  @param  boolean     are we already failed?
     */
    DeferredDelete(bool failed = false) : Deferred(failed) {}

public:
    /**
     *  Register a function to be called when the queue is deleted or purged
     *
     *  Only one callback can be registered. Successive calls
     *  to this function will clear callbacks registered before.
     *
     *  @param  callback    the callback to execute
     */
    DeferredDelete &onSuccess(const DeleteCallback &callback)
    {
        // store callback
        _deleteCallback = callback;
        
        // allow chaining
        return *this;
    }

    /**
     *  Register the function that is called when the queue is deleted or purged
     *  @param  callback
     */
    DeferredDelete &onSuccess(const SuccessCallback &callback)
    {
        // call base
        Deferred::onSuccess(callback);
        
        // allow chaining
        return *this;
    }
};

/**
 *  End namespace
 */
}
