/**
 *  Class describing an AMQP transaction commit frame
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
class TransactionCommitFrame : public TransactionFrame
{
public:
    /**
     *  Destructor
     */
    virtual ~TransactionCommitFrame() {}

    /**
     * Construct a transaction commit frame
     * 
     * @param   channel     channel identifier
     * @return  newly created transaction commit frame
     */
    TransactionCommitFrame(uint16_t channel) : 
        TransactionFrame(channel, 0)
    {}

    /**
     *  Constructor based on incoming data
     *  @param  frame   received frame
     */
    TransactionCommitFrame(ReceivedFrame &frame) :
        TransactionFrame(frame)
    {}

    /**
     *  return the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 20;
    }  
};

/**
 *  end namespace
 */
}

