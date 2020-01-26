/**
 *  Class describing a basic QOS frame
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
class BasicQosOKFrame : public BasicFrame
{
protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param      buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base, then done (no other params)
        BasicFrame::fill(buffer);
    }

public:
    /**
     *  Construct a basic qos ok frame
     *  @param  channel     channel we're working on
     */
    BasicQosOKFrame(uint16_t channel) : BasicFrame(channel, 0) {}

    /**
     *  Constructor based on incoming data
     *  @param  frame
     */
    BasicQosOKFrame(ReceivedFrame &frame) : BasicFrame(frame) {}

    /**
     *  Destructor
     */
    virtual ~BasicQosOKFrame() {}

    /**
     *  Return the method id
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
        if (!channel) return false;

        // report
        channel->reportSuccess();

        // done
        return true;
    }
};

/**
 *  End of namespace
 */
}

