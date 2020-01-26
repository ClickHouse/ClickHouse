/**
 *  Class describing connection tune acknowledgement frame
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
class ConnectionTuneOKFrame : public ConnectionFrame
{
private:
    /**
     *  Proposed maximum number of channels
     *  @var uint16_t
     */
    uint16_t _channels;

    /**
     *  Proposed maximum frame size
     *  @var uint32_t
     */
    uint32_t _frameMax;

    /**
     *  Desired heartbeat delay
     *  @var uint16_t
     */
    uint16_t _heartbeat;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ConnectionFrame::fill(buffer);

        // add fields
        buffer.add(_channels);
        buffer.add(_frameMax);
        buffer.add(_heartbeat);
    }
public:
    /**
     *  Construct a connection tune frame from a received frame
     *
     *  @param  frame   received frame
     */
    ConnectionTuneOKFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame),
        _channels(frame.nextUint16()),
        _frameMax(frame.nextUint32()),
        _heartbeat(frame.nextUint16())
    {}

    /**
     *  Construct a connection tuning acknowledgement frame
     *
     *  @param  channels        selected maximum number of channels
     *  @param  frame           selected maximum frame size
     *  @param  heartbeat       desired heartbeat delay
     */
    ConnectionTuneOKFrame(uint16_t channels, uint32_t frameMax, uint16_t heartbeat) : 
        ConnectionFrame(8), // 2x uint16_t, 1x uint32_t
        _channels(channels),
        _frameMax(frameMax),
        _heartbeat(heartbeat)
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionTuneOKFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 31;
    }

    /**
     *  Proposed maximum number of channels
     *  @return _uint16_t
     */
    uint16_t channels() const
    {
        return _channels;
    }

    /**
     *  Proposed maximum frame size
     *  @return _uint32_t
     */
    uint32_t frameMax() const
    {
        return _frameMax;
    }

    /**
     *  Desired heartbeat delay
     *  @return uint16_t
     */
    uint16_t heartbeat() const
    {
        return _heartbeat;
    }

    /**
     *  Is this a frame that is part of the connection setup?
     *  @return bool
     */
    virtual bool partOfHandshake() const override
    {
        return true;
    }
};

/**
 *  end namespace
 */
}
