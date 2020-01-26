/**
 *  Class describing an AMQP connection frame
 * 
 *  @copyright 2014 Copernica BV
 */

/**
 *  Class definition
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ConnectionFrame : public MethodFrame
{
protected:
    /**
     *  Constructor for a connectionFrame
     *  
     *  A connection frame never has a channel identifier, so this is passed
     *  as zero to the base constructor
     * 
     *  @param  size        size of the frame
     */
    ConnectionFrame(uint32_t size) : MethodFrame(0, size) {}

    /**
     *  Constructor based on a received frame
     *  @param  frame
     */
    ConnectionFrame(ReceivedFrame &frame) : MethodFrame(frame) {}

public:
    /**
     *  Destructor
     */
    virtual ~ConnectionFrame() {}

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 10;
    }
};

/**
 *  end namespace
 */
}

