/**
 *  Class describing connection close acknowledgement frame
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
class ConnectionCloseOKFrame : public ConnectionFrame
{
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
    }
public:
    /**
     *  Constructor based on a received frame
     *
     *  @param frame    received frame
     */
    ConnectionCloseOKFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame)
    {}

    /**
     *  construct a channelcloseokframe object
     */
    ConnectionCloseOKFrame() :
        ConnectionFrame(0)
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionCloseOKFrame() {}

    /**
     *  Method id
     */
    virtual uint16_t methodID() const override
    {
        return 51;
    }
    
    /**
     *  Process the frame
     *  @param  connection
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // report that it is closed
        connection->reportClosed();
        
        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

