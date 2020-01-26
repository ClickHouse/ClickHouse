/**
 *  Class describing a channel open acknowledgement frame
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
class ChannelOpenOKFrame : public ChannelFrame
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
        
        // create and encode the silly deprecated argument
        LongString unused;
        
        // add to the buffer
        unused.fill(buffer);
    }
    
public:
    /**
     *  Constructor based on client information
     *  @param  channel     Channel identifier
     */
    ChannelOpenOKFrame(uint16_t channel) : ChannelFrame(channel, 4) {} // 4 for the longstring size value

    /**
     *  Constructor based on incoming frame
     *  @param  frame
     */
    ChannelOpenOKFrame(ReceivedFrame &frame) : ChannelFrame(frame) 
    {
        // read in a deprecated argument
        LongString unused(frame);
    }
    
    /**
     *  Destructor
     */
    virtual ~ChannelOpenOKFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 11;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // we need the appropriate channel
        auto channel = connection->channel(this->channel());
        
        // channel does not exist
        if(!channel) return false;    
        
        // report that the channel is open
        channel->reportReady();
        
        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

