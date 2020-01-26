/**
 *  Class describing a basic publish frame
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
class BasicPublishFrame : public BasicFrame 
{
private:
    /**
     *  Variable that no longer is in use
     *  @var int16_t
     */
    int16_t _deprecated = 0;

    /**
     *  the name of the exchange to publish to. An empty exchange name means the default exchange.
     *  @var ShortString
     */
    ShortString _exchange;

    /**
     *  Message routing key
     *  @var ShortString
     */
    ShortString _routingKey;

    /**
     *  BooleanSet, contains:
     *  0: mandatory, indicate mandatory routing. If set, server will return unroutable message, otherwise server silently drops message
     *  1: immediate, Request immediate delivery, if set and cannot be routed, return unroutable message. 
     *  @var BooleanSet
     */
    BooleanSet _bools;

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

        // encode fields
        buffer.add(_deprecated);
        _exchange.fill(buffer);
        _routingKey.fill(buffer);
        _bools.fill(buffer);
    }

public:
    /**
     *  Construct a basic publish frame
     *
     *  @param  channel         channel we're working on
     *  @param  exchange        name of exchange to publish to   @default = ""
     *  @param  routingKey      message routing key              @default = ""
     *  @param  mandatory       indicate mandatory routing       @default = false
     *  @param  immediate       request immediate delivery       @default = false
     */
    BasicPublishFrame(uint16_t channel, const std::string& exchange = "", const std::string& routingKey = "", bool mandatory = false, bool immediate = false) :
        BasicFrame(channel, (uint32_t)(exchange.length() + routingKey.length() + 5)), // 1 extra per string (for the size), 1 for bools, 2 for deprecated field
        _exchange(exchange),
        _routingKey(routingKey),
        _bools(mandatory, immediate)
    {}

    /**
     *  Construct a basic publish frame from a received frame
     *
     *  @param frame    received frame to parse
     */
    BasicPublishFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _deprecated(frame.nextInt16()),
        _exchange(frame),
        _routingKey(frame),
        _bools(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicPublishFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    bool synchronous() const override
    {
        return false;
    }

    /**
     *  Return the name of the exchange to publish to
     *  @return  string
     */
    const std::string& exchange() const
    {
        return _exchange;
    }

    /**
     *  Return the routing key
     *  @return  string
     */
    const std::string& routingKey() const
    {
        return _routingKey;
    }

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 40;
    }

    /**
     *  Return the mandatory routing indication
     *  @return  boolean
     */
    bool mandatory() const
    {
        return _bools.get(0);
    }

    /**
     *  Return the request immediate delivery indication
     *  @return  boolean
     */
    bool immediate() const
    {
        return _bools.get(1);
    }
};

/**
 *  end namespace
 */
}

