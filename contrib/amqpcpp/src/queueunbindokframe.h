/**
 *  Class describing an AMQP queue unbind ok frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Class definition
 */
namespace AMQP {

/**
 *  Class implementation
 */
class QueueUnbindOKFrame : public QueueFrame
{
protected:
    /**
     *  Encode a queueunbindokframe on a stringbuffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        QueueFrame::fill(buffer);
    }
public:
    /**
     * Decode a queueunbindokframe from a received frame
     *
     * @param   frame   received frame to decode
     * @return  shared pointer to created frame
     */
    QueueUnbindOKFrame(ReceivedFrame& frame) :
        QueueFrame(frame)
    {}

    /**
     * construct a queueunbindokframe
     *
     * @param   channel     channel identifier
     */
    QueueUnbindOKFrame(uint16_t channel) :
        QueueFrame(channel, 0)
    {}

    /**
     *  Destructor
     */
    virtual ~QueueUnbindOKFrame() {}

    /**
     * returns the method id
     * @return  uint16_t
     */
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

        // report queue unbind success
        channel->reportSuccess();

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

