/**
 *  Class describing a channel flow frame
 * 
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ChannelFlowFrame : public ChannelFrame
{
private:
    /**
     *  Enable or disable the channel flow
     *  @var BooleanSet
     */
    BooleanSet _active;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ChannelFrame::fill(buffer);

        // add fields
        _active.fill(buffer);
    }

public:
    /**
     *  Construct a channel flow frame
     *
     *  @param  frame   received frame
     */
    ChannelFlowFrame(ReceivedFrame &frame) :    
        ChannelFrame(frame),
        _active(frame)
    {}

    /**
     *  Construct a channel flow frame
     *
     *  @param  channel     channel we're working on
     *  @param  active      enable or disable channel flow
     */
    ChannelFlowFrame(uint16_t channel, bool active) : 
        ChannelFrame(channel, 1), //sizeof bool
        _active(active)
    {}

    /**
     *  Destructor
     */
    virtual ~ChannelFlowFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 20;
    }
    
    /**
     *  Is channel flow active or not?
     *  @return bool
     */
    bool active() const
    {
        return _active.get(0);
    }
};

/**
 *  end namespace
 */
}

