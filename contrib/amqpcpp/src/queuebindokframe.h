/**
 *  Class describing an AMQP queue bind ok frame
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
class QueueBindOKFrame : public QueueFrame
{
protected:
    /**
     *  Fill output buffer
     *  @param  buffer
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        QueueFrame::fill(buffer);
    }

public:
    /**
     *  Construct a queuebindokframe
     *
     *  @param  channel     channel identifier
     */
    QueueBindOKFrame(uint16_t channel) : QueueFrame(channel, 0) {}

    /**
     *  Constructor based on incoming data
     *  @param  frame   received frame
     */
    QueueBindOKFrame(ReceivedFrame &frame) :
        QueueFrame(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~QueueBindOKFrame() {}

    /**
     *  returns the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 21;
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

/**
 *  end namespace
 */
}

