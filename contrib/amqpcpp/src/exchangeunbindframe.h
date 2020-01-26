/**
 *  Exchangeunbindframe.h
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
class ExchangeUnbindFrame : public ExchangeFrame
{
private:
    /**
     *  reserved byte
     *  @var uint16_t
     */
    uint16_t _reserved = 0;

    /**
     *  Exchange to bind to
     *  @var ShortString
     */
    ShortString _destination;

    /**
     *  Exchange which is bound
     *  @var ShortString
     */
    ShortString _source;

    /**
     *  Routing key
     *  @var ShortString
     */
    ShortString _routingKey;

    /**
     *  contains: nowait  do not wait on response
     *  @var booleanset
     */
    BooleanSet _bools;

    /**
     *  Additional arguments
     *  @var Table
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

        buffer.add(_reserved);
        _destination.fill(buffer);
        _source.fill(buffer);
        _routingKey.fill(buffer);
        _bools.fill(buffer);
        _arguments.fill(buffer);
    }

public:
    /**
     *  Constructor based on incoming data
     *
     *  @param  frame   received frame to decode
     */
    ExchangeUnbindFrame(ReceivedFrame &frame) :
        ExchangeFrame(frame),
        _reserved(frame.nextUint16()),
        _destination(frame),
        _source(frame),
        _routingKey(frame),
        _bools(frame),
        _arguments(frame)
    {}

    /**
     *  Constructor for an exchangebindframe
     *  @param  destination
     *  @param  source
     *  @param  routingkey
     *  @param  noWait
     *  @param  arguments
     */
    ExchangeUnbindFrame(uint16_t channel, const std::string &destination, const std::string &source, const std::string &routingKey, bool noWait, const Table &arguments) :
        ExchangeFrame(channel, (uint32_t)(destination.length() + source.length() + routingKey.length() + arguments.size() + 6)), // 1 for each string, 1 for booleanset, 2 for deprecated field
        _destination(destination),
        _source(source),
        _routingKey(routingKey),
        _bools(noWait),
        _arguments(arguments)
    {}

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
     *  Get the destination exchange
     *  @return string
     */
    const std::string& destination() const
    {
        return _destination;
    }

    /**
     *  Get the source exchange
     *  @return string
     */
    const std::string& source() const
    {
        return _source;
    }

    /**
     *  Get the routing key
     *  @return string
     */
    const std::string& routingkey() const
    {
        return _routingKey;
    }

    /**
     *  Get the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 40;
    }

    /**
     *  Get the additional arguments
     *  @return Table
     */
    const Table& arguments() const
    {
        return _arguments;
    }

    /**
     *  Get the nowait bool
     *  @return bool
     */
    bool noWait() const
    {
        return _bools.get(0);
    }

};
// leave namespace
}
