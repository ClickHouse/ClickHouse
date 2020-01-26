/**
 *  Class describing a basic recover-async frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class BasicRecoverOKFrame : public BasicFrame {
protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base then done, no other fields to encode
        BasicFrame::fill(buffer);
    }

public:
    /**
     *  Construct a basic recover ok frame from a received frame
     *
     *  @param frame    received frame
     */
    BasicRecoverOKFrame(ReceivedFrame &frame) :
        BasicFrame(frame)
    {}

    /**
     *  Construct a basic recover ok frame
     *
     *  @param  channel         channel id
     */
    BasicRecoverOKFrame(uint16_t channel) :
        BasicFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicRecoverOKFrame() {}

    /**
     * Return the method ID
     * @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 111;
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
