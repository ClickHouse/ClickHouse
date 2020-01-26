/**
 *  Class describing a basic cancel ok frame
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
class BasicCancelOKFrame : public BasicFrame
{
private:
    /**
     *  Holds the consumer tag specified by the client or provided by the server.
     *  @var ShortString
     */
    ShortString _consumerTag;

protected:
    /**
     * Encode a frame on a string buffer
     *
     * @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // add own information
        _consumerTag.fill(buffer);
    }

public:
    /**
     *  Construct a basic cancel ok frame
     *
     *  @param  frame   received frame
     */
    BasicCancelOKFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _consumerTag(frame)
    {}

    /**
     *  Construct a basic cancel ok frame (client-side)
     *  @param  channel     channel identifier
     *  @param  consumerTag holds the consumertag specified by client or server
     */
    BasicCancelOKFrame(uint16_t channel, std::string& consumerTag) :
        BasicFrame(channel, (uint32_t)(consumerTag.length() + 1)),    // add 1 byte for encoding the size of consumer tag
        _consumerTag(consumerTag)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicCancelOKFrame() {}

    /**
     *  Return the consumertag, which is specified by the client or provided by the server
     *  @return string
     */
    const std::string& consumerTag() const
    {
        return _consumerTag;
    }

    /**
     *  Return the method ID
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 31;
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
        channel->reportSuccess<const std::string&>(consumerTag());

        // done
        return true;
    }
};

/**
 *  End of namespace
 */
}

