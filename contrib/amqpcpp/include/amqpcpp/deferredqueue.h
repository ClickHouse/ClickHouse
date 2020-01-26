/**
 *  DeferredQueue.h
 *
 *  Deferred callback for "declare-queue" instructions.
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
class DeferredQueue : public Deferred
{
private:
    /**
     *  Callback to execute when the queue is declared
     *  @var    QueueCallback
     */
    QueueCallback _queueCallback;

    /**
     *  Report success for queue declared messages
     *  @param  name            Name of the new queue
     *  @param  messagecount    Number of messages in the queue
     *  @param  consumercount   Number of consumers linked to the queue
     *  @return Deferred        Next deferred result
     */
    virtual const std::shared_ptr<Deferred> &reportSuccess(const std::string &name, uint32_t messagecount, uint32_t consumercount) const override
    {
        // skip if no special callback was installed
        if (!_queueCallback) return Deferred::reportSuccess();
        
        // call the queue callback
        _queueCallback(name, messagecount, consumercount);
        
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
     *  @param  bool    are we already failed?
     */
    DeferredQueue(bool failed = false) : Deferred(failed) {}

public:
    /**
     *  Register a function to be called when the queue is declared
     *
     *  Only one callback can be registered. Successive calls
     *  to this function will clear callbacks registered before.
     *
     *  @param  callback    the callback to execute
     */
    DeferredQueue &onSuccess(const QueueCallback &callback)
    {
        // store callback
        _queueCallback = callback;
        
        // allow chaining
        return *this;
    }
    
    /**
     *  Register the function that is called when the queue is declared
     *  @param  callback
     */
    DeferredQueue &onSuccess(const SuccessCallback &callback)
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
