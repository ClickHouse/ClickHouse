/**
 *  Class describing an AMQP transaction select frame
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
class TransactionSelectFrame : public TransactionFrame
{
public:
    /**
     *  Decode a transaction select frame from a received frame
     *
     *  @param   frame   received frame to decode
     */
    TransactionSelectFrame(ReceivedFrame& frame) :
        TransactionFrame(frame)
    {}

    /**
     *  Construct a transaction select frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created transaction select frame
     */
    TransactionSelectFrame(uint16_t channel) :
        TransactionFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~TransactionSelectFrame() {}

    /**
     * return the method id
     * @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }    
};

/**
 *  end namespace
 */
}

