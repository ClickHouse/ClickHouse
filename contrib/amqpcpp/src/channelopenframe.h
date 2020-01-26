/**
 *  Class describing a channel open frame
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
class ChannelOpenFrame : public ChannelFrame
{
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
        
        // add deprecated data
        ShortString unused;
        
        // add to the buffer
        unused.fill(buffer);
    }

public:
    /**
     *  Constructor to create a channelOpenFrame
     *
     *  @param  channel     channel we're working on
     */
    ChannelOpenFrame(uint16_t channel) : ChannelFrame(channel, 1) {}    // 1 for the deprecated shortstring size

    /**
     *  Construct to parse a received frame
     *  @param  frame
     */
    ChannelOpenFrame(ReceivedFrame &frame) : ChannelFrame(frame)
    {
        // deprecated argument
        ShortString unused(frame);
    }

    /**
     *  Destructor
     */
    virtual ~ChannelOpenFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }
};

/**
 *  end namespace
 */
}

