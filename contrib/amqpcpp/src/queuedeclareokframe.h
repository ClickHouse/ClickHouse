/**
 *  Class describing an AMQP queue declare ok frame
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
class QueueDeclareOKFrame : public QueueFrame
{
private:
    /**
     *  The queue name
     *  @var ShortString
     */
    ShortString _name;

    /**
     *  Number of messages
     *  @var int32_t
     */
    int32_t _messageCount;

    /**
     *  Number of Consumers
     *  @var int32_t
     */
    int32_t _consumerCount;


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

        // add fields
        _name.fill(buffer);
        buffer.add(_messageCount);
        buffer.add(_consumerCount);
    }

public:
    /**
     *  Construct a channel flow frame
     *
     *  @param  channel         channel identifier
     *  @param  code            reply code
     *  @param  text            reply text
     *  @param  failingClass    failing class id if applicable
     *  @param  failingMethod   failing method id if applicable
     */
    QueueDeclareOKFrame(uint16_t channel, const std::string& name, int32_t messageCount, int32_t consumerCount) :
        QueueFrame(channel, (uint32_t)(name.length() + 9)), // 4 per int, 1 for string size
        _name(name),
        _messageCount(messageCount),
        _consumerCount(consumerCount)
    {}

    /**
     *  Constructor based on incoming data
     *  @param  frame   received frame
     */
    QueueDeclareOKFrame(ReceivedFrame &frame) :
        QueueFrame(frame),
        _name(frame),
        _messageCount(frame.nextInt32()),
        _consumerCount(frame.nextInt32())
    {}

    /**
     *  Destructor
     */
    virtual ~QueueDeclareOKFrame() {}

    /**
     *  Method id
     */
    virtual uint16_t methodID() const override
    {
        return 11;
    }

    /**
     *  Queue name
     *  @return string
     */
    const std::string& name() const
    {
        return _name;
    }

    /**
     *  Number of messages
     *  @return int32_t
     */
    uint32_t messageCount() const
    {
        return _messageCount;
    }

    /**
     *  Number of consumers
     *  @return int32_t
     */
    uint32_t consumerCount() const
    {
        return _consumerCount;
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

        // what if channel doesn't exist?
        if (!channel) return false;

        // report success
        channel->reportSuccess(name(), messageCount(), consumerCount());

        // done
        return true;
    }
};

/**
 *  End of namespace
 */
}

