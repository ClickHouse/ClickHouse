/**
 *  Class describing an AMQP transaction rollback ok frame
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
class TransactionRollbackOKFrame : public TransactionFrame
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
        TransactionFrame::fill(buffer);
    }

public:
    /**
     *  Decode a transaction rollback ok frame from a received frame
     *
     *  @param   frame   received frame to decode
     */
    TransactionRollbackOKFrame(ReceivedFrame& frame) :
        TransactionFrame(frame)
    {}

    /**
     *  Construct a transaction rollback ok frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created transaction rollback ok frame
     */
    TransactionRollbackOKFrame(uint16_t channel) :
        TransactionFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~TransactionRollbackOKFrame() {}

    /**
     * return the method id
     * @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 31;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // we need the appropriate channel
        auto channel = connection->channel(this->channel());

        // channel does not exist
        if(!channel) return false;

        // report that the channel is open
        channel->reportSuccess();

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

