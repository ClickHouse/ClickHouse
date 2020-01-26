/**
 *  Class describing a basic cancel frame
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
class BasicCancelFrame : public BasicFrame
{
private:
    /**
     *  Holds the consumer tag specified by the client or provided by the server.
     *  @var    ShortString
     */
    ShortString _consumerTag;

    /**
     *  whether to wait for a response
     *  @var    BooleanSet
     */
    BooleanSet _noWait;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // add consumer tag + booleans
        _consumerTag.fill(buffer);
        _noWait.fill(buffer);
    }

public:
    /**
     *  Construct a basic cancel frame from a received frame
     *
     *  @param  frame   received frame to parse
     */
    BasicCancelFrame(ReceivedFrame &frame) : BasicFrame(frame), _consumerTag(frame), _noWait(frame) {}
    
    /**
     *  Construct a basic cancel frame
     *
     *  @param  channel         Channel identifier    
     *  @param  consumerTag     consumertag specified by client of provided by server
     *  @param  noWait          whether to wait for a response.
     */
    BasicCancelFrame(uint16_t channel, const std::string& consumerTag, bool noWait = false) : 
        BasicFrame(channel, (uint32_t)(consumerTag.size() + 2)),    // 1 for extra string size, 1 for bool
        _consumerTag(consumerTag),
        _noWait(noWait) {}

    /**
     *  Destructor
     */
    virtual ~BasicCancelFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    bool synchronous() const override
    {
        // we are synchronous when the nowait option is not used
        return !noWait();
    }

    /**
     *  Return the consumertag, which is specified by the client or provided by the server
     *  @return  string
     */
    const std::string& consumerTag() const
    {
        return _consumerTag;
    }

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 30;
    }

    /**
     *  Return whether to wait for a response
     *  @return  boolean
     */
    bool noWait() const
    {
        return _noWait.get(0);
    }
};

/**
 *  end namespace
 */
}

