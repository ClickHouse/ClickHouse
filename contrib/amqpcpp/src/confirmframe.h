/**
 *  Class describing an AMQP confirm frame
 * 
 *  @author Marcin Gibula <m.gibula@gmail.com>
 *  @copyright 2017 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ConfirmFrame : public MethodFrame
{
protected:
    /**
     *  Constructor
     *  @param  channel     channel identifier
     *  @param  size        frame size
     */
    ConfirmFrame(uint16_t channel, uint32_t size) :
        MethodFrame(channel, size)
    {}

    /**
     *  Constructor based on incoming frame
     *  @param  frame
     */
    ConfirmFrame(ReceivedFrame &frame) :
        MethodFrame(frame)
    {}

public:
    /**
     *  Destructor
     */
    virtual ~ConfirmFrame() {}

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 85;
    }
};

/**
 *  end namespace
 */
}

