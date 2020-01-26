/**
 *  Exchangeunbindokframe.h
 *
 *  @copyright 2014 Copernica BV
 */


/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class ExchangeUnbindOKFrame : public ExchangeFrame
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
     *  Constructor based on incoming data
     *
     *  @param  frame   received frame to decode
     */
    ExchangeUnbindOKFrame(ReceivedFrame &frame) :
        ExchangeFrame(frame)
    {}

    /**
     *  Constructor for an exchangebindframe
     *  @param  destination
     *  @param  source
     *  @param  routingkey
     *  @param  noWait
     *  @param  arguments
     */
    ExchangeUnbindOKFrame(uint16_t channel) :
        ExchangeFrame(channel, 0)
    {}

    virtual uint16_t methodID() const override
    {
        return 51;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // check if we have a channel
        auto channel = connection->channel(this->channel());

        // channel does not exist
        if(!channel) return false;

        // report to handler
        channel->reportSuccess();

        // done
        return true;
    }
};

// end namespace
}
