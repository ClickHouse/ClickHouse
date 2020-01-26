/**
 *  Class describing an AMQP queue declare frame
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
class QueueDeclareFrame : public QueueFrame
{
private:
    /**
     *  Field that no longer is in use
     *  @var int16_t
     */
    int16_t _deprecated = 0;

    /**
     *  The exchange name
     *  @var ShortString
     */
    ShortString _name;

    /**
     *  Set containing all booleans
     *  0: passive      do not create queue if it does not exist
     *  1: durable      durable queue
     *  2: exclusive    request exclusive queue
     *  3: auto-delete  delete queue if unused
     *  4: noWait       don't wait on response
     *  @var BooleanSet
     */
    BooleanSet _bools;

    /**
     *  Additional arguments. Implementation dependant.
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
        QueueFrame::fill(buffer);

        // add fields
        buffer.add(_deprecated);
        _name.fill(buffer);
        _bools.fill(buffer);
        _arguments.fill(buffer);
    }

public:
    /**
     *  Destructor
     */
    virtual ~QueueDeclareFrame() {}

    /**
     *  Construct a channel flow frame
     *
     *  @param  channel             channel identifier
     *  @param  String name         Name of the queue
     *  @param  Bool passive        passive declaration, do not create queue if it does not exist
     *  @param  Bool durable        whether to create a durable queue
     *  @param  Bool autoDelete     automatically delete queue when it is empty
     *  @param  Bool noWait         whether to wait for a return value
     *  @param  Table arguments     additional arguments, implementation dependent
     */
    QueueDeclareFrame(uint16_t channel, const std::string& name = "", bool passive = false, bool durable = false, bool exclusive = false, bool autoDelete = false, bool noWait = false, const Table& arguments = {}) :
        QueueFrame(channel, (uint32_t)(name.length() + arguments.size() + 4 ) ), // 1 extra for string size, 1 for bools, 2 for deprecated value
        _name(name),
        _bools(passive, durable, exclusive, autoDelete, noWait),
        _arguments(arguments)
    {}
    
    /**
     *  Constructor based on incoming data
     *  @param  frame   received frame
     */
    QueueDeclareFrame(ReceivedFrame &frame) :
        QueueFrame(frame),
        _deprecated(frame.nextInt16()),
        _name(frame),
        _bools(frame),
        _arguments(frame)
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
     *  returns the method id
     *  @return string
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }

    /**
     *  return the queue name
     *  @return string
     */
    const std::string& name() const
    {
        return _name;
    }

    /**
     *  returns value of passive declaration, do not create queue if it does not exist
     *  @return bool
     */
    bool passive() const
    {
        return _bools.get(0);
    }

    /**
     *  returns whether the queue is durable
     *  @return bool
     */
    bool durable() const
    {
        return _bools.get(1);
    }

    /**
     *  returns whether the queue is exclusive
     *  @return bool
     */
    bool exclusive() const
    {
        return _bools.get(2);
    }

    /**
     *  returns whether the queue is deleted if unused
     *  @return bool
     */
    bool autoDelete() const
    {
        return _bools.get(3);
    }

    /**
     *  returns whether to  wait for a response
     *  @return bool
     */
    bool noWait() const
    {
        return _bools.get(4);
    }

    /**
     *  returns additional arguments. Implementation dependant.
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

