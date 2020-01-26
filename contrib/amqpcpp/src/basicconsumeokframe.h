/**
 *  Class describing a basic consume ok frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP{

/**
 *  Class implementation
 */
class BasicConsumeOKFrame : public BasicFrame
{
private:
    /**
     *  Holds the consumer tag specified by the client or provided by the server.
     *  @var ShortString
     */
    ShortString _consumerTag;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // add payload
        _consumerTag.fill(buffer);
    }

public:
    /**
     *  Construct a basic consume frame
     *
     *  @param  consumerTag       consumertag specified by client of provided by server
     */
    BasicConsumeOKFrame(uint16_t channel, const std::string& consumerTag) :
        BasicFrame(channel, (uint32_t)(consumerTag.length() + 1)), // length of string + 1 for encoding of stringsize
        _consumerTag(consumerTag)
    {}

    /**
     *  Construct a basic consume ok frame from a received frame
     *
     *  @param frame    received frame
     */
    BasicConsumeOKFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _consumerTag(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicConsumeOKFrame() {}

    /**
     *  Return the method ID
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 21;
    }

    /**
     *  Return the consumertag, which is specified by the client or provided by the server
     *  @return std::string
     */
    const std::string& consumerTag() const
    {
        return _consumerTag;
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
        channel->reportSuccess(consumerTag());

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

