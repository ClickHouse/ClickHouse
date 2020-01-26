/**
 *  Class describing a basic QOS frame
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
class BasicQosFrame : public BasicFrame
{
private:
    /**
     *  specifies the size of the prefetch window in octets
     *  @var int32_t
     */
    int32_t _prefetchSize;

    /**
     *  specifies a prefetch window in terms of whole messages
     *  @var int16_t
     */
    int16_t _prefetchCount;

    /**
     *  apply QoS settings to entire connection
     *  @var BooleanSet
     */
    BooleanSet _global;

protected:
    /**
     *  Encode a frame on a string buffer
     *  @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // add fields
        buffer.add(_prefetchSize);
        buffer.add(_prefetchCount);
        _global.fill(buffer);
    }

public:
    /**
     * Construct a basic qos frame
     *
     * @param   channel         channel we're working on
     * @param   prefetchCount   specifies a prefetch window in terms of whole messages
     * @param   global          share prefetch count with all consumers on the same channel
     * @default false
     */
    BasicQosFrame(uint16_t channel, int16_t prefetchCount = 0, bool global = false) :
        BasicFrame(channel, 7), // 4 (int32) + 2 (int16) + 1 (bool)
        _prefetchSize(0),
        _prefetchCount(prefetchCount),
        _global(global)
    {}

    /**
     *  Constructor based on incoming frame
     *  @param  frame
     */
    BasicQosFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _prefetchSize(frame.nextInt32()),
        _prefetchCount(frame.nextInt16()),
        _global(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicQosFrame() {}

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }

    /**
     *  Return the prefetch count
     *  @return int16_t
     */
    int16_t prefetchCount() const
    {
        return _prefetchCount;
    }

    /**
     *  returns the value of global
     *  @return  boolean
     */
    bool global() const
    {
        return _global.get(0);
    }

    /**
     *  returns the prefetch size
     *  @return int32_t
     */
    int32_t prefetchSize() const
    {
        return _prefetchSize;
    }
};

/**
 *  End namespace
 */
}

