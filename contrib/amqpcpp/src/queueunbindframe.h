/**
 *  Class describing an AMQP queue bind frame
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
class QueueUnbindFrame : public QueueFrame
{
private:
    /**
     *  Unused field
     *  @var int16_t
     */
    int16_t _deprecated = 0;

    /**
     *  the queue name
     *  @var ShortString
     */
    ShortString _name;

    /**
     *  the exchange name
     *  @var ShortString
     */
    ShortString _exchange;

    /**
     *  the routing key
     *  @var ShortString
     */
    ShortString _routingKey;

    /**
     *  additional arguments, implementation dependant.
     *  @var Table
     */
    Table _arguments;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        QueueFrame::fill(buffer);

        // add fields
        buffer.add(_deprecated);
        _name.fill(buffer);
        _exchange.fill(buffer);
        _routingKey.fill(buffer);
        _arguments.fill(buffer);
    }

public:
    /**
     *  Destructor
     */
    virtual ~QueueUnbindFrame() {}

    /**
     *  Construct a queueunbindframe
     *
     *  @param   channel     channel identifier
     *  @param   name        name of the queue
     *  @param   exchange    name of the exchange
     *  @param   routingKey  the routingKey
     *  @param   arguments   additional arguments, implementation dependant.
     */
    QueueUnbindFrame(uint16_t channel, const std::string& name, const std::string& exchange, const std::string& routingKey = "", const Table& arguments = {} ) :
        QueueFrame(channel, (uint32_t)(name.length() + exchange.length() + routingKey.length() + arguments.size() + 5) ), // 1 per string, 2 for deprecated field
        _name(name),
        _exchange(exchange),
        _routingKey(routingKey),
        _arguments(arguments)
    {}

    /**
     *  Constructor based on incoming data
     *
     *  @param   frame       received frame to decode
     */
    QueueUnbindFrame(ReceivedFrame& frame) :
        QueueFrame(frame),
        _deprecated(frame.nextInt16()),
        _name(frame),
        _exchange(frame),
        _routingKey(frame),
        _arguments(frame)
    {}

    /**
     *  returns the method id
     *  @returns uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 50;
    }

    /**
     *  returns the queue name
     *  @returns string
     */
    const std::string& name() const
    {
        return _name;
    }

    /**
     *  returns the exchange name
     *  @returns string
     */
    const std::string& exchange() const
    {
        return _exchange;
    }

    /**
     *  returns the routingKey
     *  @returns string
     */
    const std::string& routingKey() const
    {
        return _routingKey;
    }

    /** 
     *  returns the additional arguments
     *  @returns Table
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

