/**
 *  Class describing connection tune frame
 * 
 *  A connection tune frame is sent by the server after the connection is
 *  started to negotiate the max frame size, the max number of channels
 *  and the heartbeat interval.
 * 
 *  When this frame is received, we should send back a tune-ok frame to
 *  confirm that we have received this frame.
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
class ConnectionTuneFrame : public ConnectionFrame
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
     *  Construct a connection tuning frame
     *
     *  @param  channels    proposed maximum number of channels
     *  @param  frameMax    proposed maximum frame size
     *  @param  heartbeat   desired heartbeat delay
     */
    ConnectionTuneFrame(uint16_t channels, uint32_t frameMax, uint16_t heartbeat) : 
        ConnectionFrame(8), // 2x uint16_t, 1x uint32_t
        _channels(channels),
        _frameMax(frameMax),
        _heartbeat(heartbeat)
    {}

    /**
     *  Construct a connection tune frame from a received frame
     *
     *  @param  frame   received frame
     */
    ConnectionTuneFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame),
        _channels(frame.nextUint16()),
        _frameMax(frame.nextUint32()),
        _heartbeat(frame.nextUint16())
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionTuneFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 30;
    }

    /**
     *  Proposed maximum number of channels
     *  @return _uint16_t
     */
    uint16_t channelMax() const
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
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // remember this in the connection
        connection->setCapacity(channelMax(), frameMax());
        
        // theoretically it is possible that the connection object gets destructed between sending the messages
        Monitor monitor(connection);
        
        // store the heartbeat the server wants 
        uint16_t interval = connection->setHeartbeat(heartbeat());

        // send it back
        connection->send(ConnectionTuneOKFrame(channelMax(), frameMax(), interval));
        
        // check if the connection object still exists
        if (!monitor.valid()) return true;
        
        // and finally we start to open the frame
        return connection->send(ConnectionOpenFrame(connection->vhost()));
    }
};

/**
 *  end namespace
 */
}

