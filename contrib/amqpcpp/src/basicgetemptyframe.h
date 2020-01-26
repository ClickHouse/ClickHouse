/**
 *  Class describing a basic get empty frame
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
class BasicGetEmptyFrame : public BasicFrame 
{
private:
    /**
     *  Field that is no longer used
     *  @var ShortString
     */
    ShortString _deprecated;

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

        // recreate deprecated field and encode
        _deprecated.fill(buffer);
    }


public:
    /**
     *  Construct a basic get empty frame
     *
     *  @param  channel     channel we're working on
     */
    BasicGetEmptyFrame(uint16_t channel) :
        BasicFrame(channel, 1)  // 1 for encoding the deprecated cluster id (shortstring)
    {}

    /**
     *  Constructor for incoming data
     *  @param  frame   received frame
     */
    BasicGetEmptyFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _deprecated(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicGetEmptyFrame() {}

    /**
     * Return the method ID
     * @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 72;
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
 *  end namespace
 */
}

