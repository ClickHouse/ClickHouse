/**
 *  Class describing an AMQP exchange frame
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
class ExchangeFrame : public MethodFrame
{
protected:
    /**
     *  Constructor based on incoming data
     *
     *  @param  frame   received frame to decode
     */
    ExchangeFrame(ReceivedFrame &frame) : MethodFrame(frame) {}

    /**
     *  Constructor for an exchange frame
     *
     *  @param  channel     channel we're working on
     *  @param  size        size of the payload
     */
    ExchangeFrame(uint16_t channel, uint32_t size) : MethodFrame(channel, size) {}

public:
    /**
     *  Destructor
     */
    virtual ~ExchangeFrame() {}

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 40;
    }
};

/**
 *  end namespace
 */
}

