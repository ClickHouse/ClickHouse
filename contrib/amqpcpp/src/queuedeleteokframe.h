/**
 *  Class describing an AMQP queue delete frame
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
class QueueDeleteOKFrame : public QueueFrame
{
private:
    /**
     *  The message count
     *  @var int32_t
     */
    int32_t _messageCount;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        QueueFrame::fill(buffer);

        // add field
        buffer.add(_messageCount);
    }

public:
    /**
     *  Construct a queuedeleteokframe
     *
     *  @param  channel         channel identifier
     *  @param  messageCount    number of messages
     */
    QueueDeleteOKFrame(uint16_t channel, int32_t messageCount) :
        QueueFrame(channel, 4), // 4 for int32_t size
        _messageCount(messageCount)
    {}

    /**
     *  Constructor based on received data
     *  @param  frame   received frame
     */
    QueueDeleteOKFrame(ReceivedFrame &frame) :
        QueueFrame(frame),
        _messageCount(frame.nextInt32())
    {}

    /**
     *  Destructor
     */
    virtual ~QueueDeleteOKFrame() {}

    /**
     *  return the method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 41;
    }

    /**
     *  returns the number of messages
     *  @return uint32_t
     */
    uint32_t messageCount() const
    {
        return _messageCount;
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

        // report queue deletion success
        channel->reportSuccess(this->messageCount());

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

