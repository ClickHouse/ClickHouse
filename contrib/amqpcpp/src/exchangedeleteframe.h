/**
 *  Class describing an AMQP exchange delete frame
 *  
 *  @copyright 2014 Copernica BV
 */

/**
 *  we live in the copernica namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ExchangeDeleteFrame : public ExchangeFrame
{
private:
    /**
     *  Field that is no longer in use
     *  @var uint16_t
     */
    uint16_t _deprecated = 0;

    /**
     *  The exchange name
     *  @var ShortString
     */
    ShortString _name;

    /**
     *  booleanset, contains:
     *  0: ifUnused
     *  1: noWait
     *  @var BooleanSet
     */
    BooleanSet _bools;

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
        _bools.fill(buffer);
    }
public:
    /**
     *  constructor based on incoming data
     *  
     *  @param  frame   received frame
     */
    ExchangeDeleteFrame(ReceivedFrame &frame) : 
        ExchangeFrame(frame),
        _deprecated(frame.nextUint16()),
        _name(frame),
        _bools(frame)
    {}

    /**
     *  construct a exchangedeleteframe
     *  
     *  @param  channel         channel we're working on
     *  @param  String name     Name of the exchange
     *  @param  bool ifUnused   Delete only if frame is not used
     *  @param  bool noWait     Do not wait for a response
     */
    ExchangeDeleteFrame(uint16_t channel, const std::string& name, bool ifUnused = false, bool noWait = false) :
        ExchangeFrame(channel, (uint32_t)(name.length() + 4)), // length of the name, 1 byte for encoding this length, 1 for bools, 2 for deprecated short
        _name(name),
        _bools(ifUnused, noWait)
    {}

    /**
     *  Destructor
     */
    virtual ~ExchangeDeleteFrame() {}

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
     *  returns the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 20;
    }

    /**
     *  returns the exchange name
     *  @return string
     */
    const std::string& name() const
    {
        return _name;
    }

    /**
     *  returns whether to delete if unused
     *  @return bool
     */
    bool ifUnused() const
    {
        return _bools.get(0);
    }

    /**
     *  returns whether to wait for a response
     *  @return bool
     */
    bool noWait() const
    {
        return _bools.get(1);
    }

};

/**
 *  end namespace
 */
}
