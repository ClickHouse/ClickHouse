/**
 *  Class describing an AMQP channel frame
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
class ChannelFrame : public MethodFrame
{
protected:
    /**
     *  Constructor for a channelFrame
     *  
     *  @param  channel     channel we're working on
     *  @param  size        size of the frame
     */
    ChannelFrame(uint16_t channel, uint32_t size) : MethodFrame(channel, size) {}

    /**
     *  Constructor that parses an incoming frame
     *  @param  frame       The received frame
     */
    ChannelFrame(ReceivedFrame &frame) : MethodFrame(frame) {}

public:
    /**
     *  Destructor
     */
    virtual ~ChannelFrame() {} 

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 20;
    }
};

/**
 *  end namespace
 */
}

