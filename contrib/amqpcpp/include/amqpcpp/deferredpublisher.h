/**
 *  DeferredPublisher.h
 * 
 *  Class that is returned when channel::publish() is called, and that
 *  can be used to install callback methods that define how returned
 *  messages should be handled.
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {
    
/**
 *  Forward declarations
 */
class ChannelImpl;

/**
 *  Class definition
 */
class DeferredPublisher : public DeferredReceiver, public std::enable_shared_from_this<DeferredPublisher>
{
private:
    /**
     *  The error code
     *  @var int16_t
     */
    int16_t _code = 0;
    
    /**
     *  The error message
     *  @var std::string
     */
    std::string _description;

    /**
     *  Callback that is called when a message is returned
     *  @var BounceCallback
     */
    BounceCallback _bounceCallback;

    /**
     *  Begin of a bounced message
     *  @var ReturnCallback
     */
    ReturnCallback _beginCallback;
    
    /**
     *  End of a bounced message
     *  @var ReturnedCallback
     */
    ReturnedCallback _completeCallback;

    /**
     *  Process a return frame
     *
     *  @param  frame   The frame to process
     */
    void process(BasicReturnFrame &frame);

    /**
     *  Get reference to self to prevent that object falls out of scope
     *  @return std::shared_ptr
     */
    virtual std::shared_ptr<DeferredReceiver> lock() override { return shared_from_this(); }

    /**
     *  Extended implementation of the complete method that is called when a message was fully received
     */
    virtual void complete() override;
    
    /**
     *  Classes that can access private members
     */
    friend class BasicReturnFrame;

public:
    /**
     *  Constructor that should only be called from within the channel implementation
     *
     *  Note: this constructor _should_ be protected, but because make_shared
     *  will then not work, we have decided to make it public after all,
     *  because the work-around would result in not-so-easy-to-read code.
     *
     *  @param  channel     the channel implementation
     *  @param  failed      are we already failed?
     */
    DeferredPublisher(ChannelImpl *channel, bool failed = false) :
        DeferredReceiver(failed, channel) {}
        
public:
    /**
     *  Register a function to be called when a full message is returned
     *  @param  callback    the callback to execute
     */
    DeferredPublisher &onReceived(const BounceCallback &callback)
    {
        // store callback
        _bounceCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Alias for onReceived() (see above)
     *  @param  callback    the callback to execute
     */
    DeferredPublisher &onMessage(const BounceCallback &callback)
    {
        // store callback
        _bounceCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Alias for onReceived() (see above)
     *  @param  callback    the callback to execute
     */
    DeferredPublisher &onReturned(const BounceCallback &callback)
    {
        // store callback
        _bounceCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Alias for onReceived() (see above)
     *  @param  callback    the callback to execute
     */
    DeferredPublisher &onBounced(const BounceCallback &callback)
    {
        // store callback
        _bounceCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Register the function that is called when the start frame of a new 
     *  consumed message is received
     *
     *  @param  callback    The callback to invoke
     *  @return Same object for chaining
     */
    DeferredPublisher &onBegin(const ReturnCallback &callback)
    {
        // store callback
        _beginCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Register a function that is called when the message size is known
     * 
     *  @param  callback    The callback to invoke for message headers
     *  @return Same object for chaining
     */
    DeferredPublisher &onSize(const SizeCallback &callback)
    {
        // store callback
        _sizeCallback = callback;
        
        // allow chaining
        return *this;
    }

    /**
     *  Register the function that is called when message headers come in
     *
     *  @param  callback    The callback to invoke for message headers
     *  @return Same object for chaining
     */
    DeferredPublisher &onHeaders(const HeaderCallback &callback)
    {
        // store callback
        _headerCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Register the function to be called when a chunk of data comes in
     *
     *  Note that this function may be called zero, one or multiple times
     *  for each incoming message depending on the size of the message data.
     *
     *  If you install this callback you very likely also want to install
     *  the onComplete callback so you know when the last data part was
     *  received.
     *
     *  @param  callback    The callback to invoke for chunks of message data
     *  @return Same object for chaining
     */
    DeferredPublisher &onData(const DataCallback &callback)
    {
        // store callback
        _dataCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Register a funtion to be called when a message was completely received
     *
     *  @param  callback    The callback to invoke
     *  @return Same object for chaining
     */
    DeferredPublisher &onComplete(const ReturnedCallback &callback)
    {
        // store callback
        _completeCallback = callback;

        // allow chaining
        return *this;
    }
};
    
/**
 *  End of namespace
 */
}

