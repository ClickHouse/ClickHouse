/**
 *  Class describing an AMQP transaction select ok frame
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
class TransactionSelectOKFrame : public TransactionFrame
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
     *  Constructor for an incoming frame
     *
     *  @param   frame   received frame to decode
     */
    TransactionSelectOKFrame(ReceivedFrame& frame) :
        TransactionFrame(frame)
    {}

    /**
     *  Construct a transaction select ok frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created transaction select ok frame
     */
    TransactionSelectOKFrame(uint16_t channel) :
        TransactionFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~TransactionSelectOKFrame() {}

    /**
     *  return the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 11;
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

