/**
 *  Class describing an AMQP queue delete frame
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
class QueueDeleteFrame : public QueueFrame
{
private:
    /**
     *  Field that is no longer in use
     *  @var int16_t
     */
    int16_t _deprecated = 0;

    /**
     *  the queue name
     *  @var ShortString
     */
    ShortString _name;

    /**
     *  Booleanset, contains
     *  0: ifUnused, delete only if unused
     *  1: ifEmpty,  delete only if empty
     *  2: noWait,   do not wait on response
     *  @var BooleanSet
     */
    BooleanSet _bools;

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
        _bools.fill(buffer);
    }

public:
    /**
     *  Destructor
     */
    virtual ~QueueDeleteFrame() {}

    /**
     *  Construct a queuedeleteframe
     *
     *  @param   channel     channel identifier
     *  @param   name        name of the queue
     *  @param   ifUnused    delete only if unused
     *  @param   ifEmpty     delete only if empty
     *  @param   noWait      do not wait on response
     */
    QueueDeleteFrame(uint16_t channel, const std::string& name, bool ifUnused = false, bool ifEmpty = false, bool noWait = false) :
        QueueFrame(channel, (uint32_t)(name.length() + 4)), // 1 for string length, 1 for bools, 2 for deprecated field
        _name(name),
        _bools(ifUnused, ifEmpty, noWait)
    {}
    
    /**
     *  Constructor based on received data
     *  @param  frame   received frame
     */
    QueueDeleteFrame(ReceivedFrame &frame) :
        QueueFrame(frame),
        _deprecated(frame.nextInt16()),
        _name(frame),
        _bools(frame)
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
     *  @returns uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 40;
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
     *  returns value of ifUnused
     *  @returns bool
     */
    bool ifUnused() const
    {
        return _bools.get(0);
    }

    /**
     *  returns the value of ifEmpty
     *  @returns bool
     */
    bool ifEmpty() const
    {
        return _bools.get(1);
    }

    /**
     *  returns the value of noWait
     *  @returns bool
     */
    bool noWait() const
    {
        return _bools.get(2);
    }
};

/**
 *  End of namespace
 */
}

