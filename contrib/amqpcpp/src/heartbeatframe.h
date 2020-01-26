/**
 *  Class describing an AMQP Heartbeat Frame
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
class HeartbeatFrame : public ExtFrame 
{
public:
    /**
     *  Construct a heartbeat frame
     *
     *  @param  channel     channel identifier
     *  @param  payload     payload of the body
     */
    HeartbeatFrame() :
        ExtFrame(0, 0)
    {}

    /**
     *  Decode a received frame to a frame
     *
     *  @param  frame   received frame to decode
     *  @return shared pointer to newly created frame
     */
    HeartbeatFrame(ReceivedFrame& frame) :
        ExtFrame(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~HeartbeatFrame() {}

    /**
     *  Return the type of frame
     *  @return     uint8_t
     */ 
    virtual uint8_t type() const override
    {
        // the documentation says 4, rabbitMQ sends 8
        return 8;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // notify the connection-handler
        connection->reportHeartbeat();

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}
