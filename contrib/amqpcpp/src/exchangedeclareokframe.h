/**
 *  Class describing an AMQP exchange declare ok frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Class definition
 */
namespace AMQP{

/**
 * Class implementation
 */
class ExchangeDeclareOKFrame : public ExchangeFrame
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
        ExchangeFrame::fill(buffer);
    }
public:
    /**
     *  Construct an exchange declare ok frame
     *
     *  @param  channel     channel we're working on
     */
    ExchangeDeclareOKFrame(uint16_t channel) : ExchangeFrame(channel, 0) {}

    /**
     *  Decode an exchange declare acknowledgement frame
     *
     *  @param  frame   received frame to decode
     */
    ExchangeDeclareOKFrame(ReceivedFrame &frame) :
        ExchangeFrame(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~ExchangeDeclareOKFrame() {}

    /**
     *  returns the method id
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

        // report exchange declare ok
        channel->reportSuccess();

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}
