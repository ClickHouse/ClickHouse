/**
 *  Class describing a basic get frame
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
class BasicGetFrame : public BasicFrame 
{
private:
    /**
     *  Deprecated field
     *  @var uint16_t
     */
    uint16_t _deprecated = 0;

    /**
     *  name of the queue to get a message from
     *  @var ShortString
     */
    ShortString _queue;

    /**
     *  if set, server does not expect acknowledgement for messages. Server dequeues message after sending
     *  @var BooleanSet
     */
    BooleanSet _noAck;

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

        // encode other values
        buffer.add(_deprecated);
        _queue.fill(buffer);
        _noAck.fill(buffer);
    }

public:
    /**
     *  Construct a basic get frame
     *
     *  @param  channel      channel we're working on
     *  @param  queue        name of the queue
     *  @param  noAck        whether server expects acknowledgements for messages     
     */
    BasicGetFrame(uint16_t channel, const std::string& queue, bool noAck = false) :
        BasicFrame(channel, (uint32_t)(queue.length() + 4)), // 1 for bool, 1 for string size, 2 for deprecated field
        _queue(queue),
        _noAck(noAck)
    {}

    /**
     *  Constructor based on incoming frame
     *  @param  frame
     */
    BasicGetFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _deprecated(frame.nextUint16()),
        _queue(frame),
        _noAck(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicGetFrame() {}

    /**
     *  Return the name of the queue
     *  @return string
     */
    const std::string& queue() const
    {
        return _queue;
    }

    /**
     *  Return the method ID
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 70;
    }

    /**
     *  Return whether the server expects acknowledgements for messages
     *  @return  boolean
     */
    bool noAck() const
    {
        return _noAck.get(0);
    }

};

/**
 *  end namespace
 */
}

