/**
 *  Class describing an AMQP transaction rollback frame
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
class TransactionRollbackFrame : public TransactionFrame
{
public:
    /**
     *  Destructor
     */
    virtual ~TransactionRollbackFrame() {}

    /**
     *  Decode a transaction rollback frame from a received frame
     *
     *  @param   frame   received frame to decode
     */
    TransactionRollbackFrame(ReceivedFrame& frame) :
        TransactionFrame(frame)
    {}

    /**
     *  Construct a transaction rollback frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created transaction rollback frame
     */
    TransactionRollbackFrame(uint16_t channel) :
        TransactionFrame(channel, 0)
    {}

    /**
     *  return the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 30;
    }
};

/**
 *  end namespace
 */
}

