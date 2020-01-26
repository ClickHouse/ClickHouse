/**
 *  Class describing an AMQP confirm select ok frame
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
class ConfirmSelectOKFrame : public ConfirmFrame
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
        ConfirmFrame::fill(buffer);
    }

public:
    /**
     *  Constructor for an incoming frame
     *
     *  @param   frame   received frame to decode
     */
    ConfirmSelectOKFrame(ReceivedFrame& frame) :
        ConfirmFrame(frame)
    {}

    /**
     *  Construct a confirm select ok frame
     * 
     *  @param   channel     channel identifier
     *  @return  newly created confirm select ok frame
     */
    ConfirmSelectOKFrame(uint16_t channel) :
        ConfirmFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~ConfirmSelectOKFrame() {}

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

