/**
 *  Class describing an AMQP exchange declare frame
 * 
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ExchangeDeclareFrame : public ExchangeFrame
{
private:
    /**
     *  Field that no longer is used
     *  @var uint16_t
     */
    uint16_t _deprecated = 0;
    
    /**
     *  The exchange name
     *  @var    ShortString
     */
    ShortString _name;

    /**
     *  The exchange type
     *  @var    ShortString
     */
    ShortString _type;

    /**
     *  The boolean set contains three settings: 
     *      0:  Passive declaration, do not create exchange if it does not exist
     *      1:  Durable exchange
     *      4:  Do not wait for a response
     *  @var    BooleanSet
     */
    BooleanSet _bools;

    /**
     *  Additional arguments. Implementation dependent.
     *  @var    Table
     */
    Table _arguments;


protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ExchangeFrame::fill(buffer);

        // add fields
        buffer.add(_deprecated);
        _name.fill(buffer);
        _type.fill(buffer);
        _bools.fill(buffer);
        _arguments.fill(buffer);
    }

public:
    /**
     *  Construct a exchange declare frame (client side)
     *
     *  @param  channel     channel we´re working on
     *  @param  name        exchange name
     *  @param  type        exchange type
     *  @param  passive     do not create exchange if it does not exist
     *  @param  durable     durable exchange
     *  @param  autodelete  is this an auto-delete exchange?
     *  @param  internal    is this an internal exchange
     *  @param  noWait      do not wait on response
     *  @param  arguments   additional arguments
     */
    ExchangeDeclareFrame(uint16_t channel, const std::string& name, const char *type, bool passive, bool durable, bool autodelete, bool internal, bool nowait, const Table& arguments) :
        ExchangeFrame(channel, (uint32_t)(name.length() + strlen(type) + arguments.size() + 5)), // size of name, type and arguments + 1 (all booleans are stored in 1 byte) + 2 (deprecated short) + 2 (string sizes)
        _name(name),
        _type(type),
        _bools(passive, durable, autodelete, internal, nowait),
        _arguments(arguments) {}

    /**
     *  Construct parsing a declare frame from a received frame
     *  @param  frame           The received frame
     */
    ExchangeDeclareFrame(ReceivedFrame &frame) : 
        ExchangeFrame(frame),
        _deprecated(frame.nextUint16()),
        _name(frame),
        _type(frame),
        _bools(frame),
        _arguments(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~ExchangeDeclareFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    bool synchronous() const override
    {
        // we are synchronous without the nowait option
        return !noWait();
    }

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }

    /**
     *  The exchange name
     *  @return string
     */
    const std::string& name() const
    {
        return _name;
    }

    /**
     *  The exchange type
     *  @return string
     */
    const std::string &exchangeType() const
    {
        return _type;
    }

    /**
     *  Passive declaration, do not create exchange if it does not exist
     *  @return bool
     */
    bool passive() const
    {
        return _bools.get(0);
    }

    /**
     *  Durable exchange
     *  @return bool
     */
    bool durable() const
    {
        return _bools.get(1);
    }
    
    /**
     *  Is this an autodelete exchange?
     *  @return bool
     */
    bool autoDelete() const
    {
        return _bools.get(2);
    }
    
    /**
     *  Is this an internal exchange?
     *  @return bool
     */
    bool internal() const
    {
        return _bools.get(3);
    }

    /**
     *  Do not wait for a response
     *  @return bool
     */
    bool noWait() const
    {
        return _bools.get(4);
    }

    /**
     *  Additional arguments. Implementation dependent.
     *  @return Table
     */
    const Table& arguments() const
    {
        return _arguments;
    }
};

/**
 *  end namespace
 */
}

