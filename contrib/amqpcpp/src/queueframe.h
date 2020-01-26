/**
 *  Class describing an AMQP queue frame
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
class QueueFrame : public MethodFrame
{
protected:
    /**
     *  Construct a queueframe
     *  @param  channel  channel identifier
     *  @param   size     size of the frame
     */
    QueueFrame(uint16_t channel, uint32_t size) : MethodFrame(channel, size) {}

    /**
     *  Constructor based on incoming data
     *  @param  frame   received frame
     */
    QueueFrame(ReceivedFrame &frame) : MethodFrame(frame) {}

public:
    /**
     *  Destructor
     */
    virtual ~QueueFrame() {}
    
    /**
     *  returns the class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 50;
    }
};

/**
 *  end namespace
 */
}

