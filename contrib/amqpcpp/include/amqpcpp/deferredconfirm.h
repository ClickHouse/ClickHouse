/**
 *  DeferredConfirm.h
 *
 *  Deferred callback for RabbitMQ-specific publisher confirms mechanism.
 *
 *  @author Marcin Gibula <m.gibula@gmail.com>
 *  @copyright 2018 Copernica BV
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
class DeferredConfirm : public Deferred
{
private:
    /**
     *  Callback to execute when server confirms that message is processed
     *  @var    AckCallback
     */
    AckCallback _ackCallback;

    /**
     * Callback to execute when server sends negative acknowledgement
     * @var     NackCallback
     */
    NackCallback _nackCallback;

    /**
     *  Process an ACK frame
     *
     *  @param  frame   The frame to process
     */
    void process(BasicAckFrame &frame);

    /**
     *  Process an ACK frame
     *
     *  @param  frame   The frame to process
     */
    void process(BasicNackFrame &frame);

    /**
     *  The channel implementation may call our
     *  private members and construct us
     */
    friend class ChannelImpl;
    friend class BasicAckFrame;
    friend class BasicNackFrame;

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
    DeferredConfirm(bool failed = false) : Deferred(failed) {}

public:
    /**
     *  Register the function that is called when channel is put in publisher
     *  confirmed mode
     *  @param  callback
     */
    DeferredConfirm &onSuccess(const SuccessCallback &callback)
    {
        // call base
        Deferred::onSuccess(callback);

        // allow chaining
        return *this;
    }

    /**
     *  Callback that is called when the broker confirmed message publication
     *  @param  callback    the callback to execute
     */
    DeferredConfirm &onAck(const AckCallback &callback)
    {
        // store callback
        _ackCallback = callback;

        // allow chaining
        return *this;
    }

    /**
     *  Callback that is called when the broker denied message publication
     *  @param  callback    the callback to execute
     */
    DeferredConfirm &onNack(const NackCallback &callback)
    {
        // store callback
        _nackCallback = callback;

        // allow chaining
        return *this;
    }
};

/**
 *  End namespace
 */
}
