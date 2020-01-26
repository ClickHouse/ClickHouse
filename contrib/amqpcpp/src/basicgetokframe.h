/**
 *  Class describing a basic get ok frame
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP{

/**
 *  Class implementation
 */
class BasicGetOKFrame : public BasicFrame
{
private:
    /**
     *  server-assigned and channel specific delivery tag
     *  @var uint64_t
     */
    uint64_t _deliveryTag;

    /**
     *  indicates whether the message has been previously delivered to this (or another) client
     *  @var BooleanSet
     */
    BooleanSet _redelivered;

    /**
     *  the name of the exchange to publish to. An empty exchange name means the default exchange.
     *  @var ShortString
     */
    ShortString _exchange;

    /**
     *  Message routing key
     *  @var ShortString
     */
    ShortString _routingKey;

    /**
     *  number of messages in the queue
     *  @var uint32_t
     */
    uint32_t _messageCount;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // encode rest of the fields
        buffer.add(_deliveryTag);
        _redelivered.fill(buffer);
        _exchange.fill(buffer);
        _routingKey.fill(buffer);
        buffer.add(_messageCount);
    }

public:
    /**
     *  Construct a basic get ok frame
     *
     *  @param  channel         channel we're working on
     *  @param  deliveryTag     server-assigned and channel specific delivery tag
     *  @param  redelivered     indicates whether the message has been previously delivered to this (or another) client
     *  @param  exchange        name of exchange to publish to
     *  @param  routingKey      message routing key
     *  @param  messageCount    number of messages in the queue
     */
    BasicGetOKFrame(uint16_t channel, uint64_t deliveryTag, bool redelivered, const std::string& exchange, const std::string& routingKey, uint32_t messageCount) :
        BasicFrame(channel, (uint32_t)(exchange.length() + routingKey.length() + 15)), // string length, +1 for each shortsrting length + 8 (uint64_t) + 4 (uint32_t) + 1 (bool)
        _deliveryTag(deliveryTag),
        _redelivered(redelivered),
        _exchange(exchange),
        _routingKey(routingKey),
        _messageCount(messageCount)
    {}

    /**
     *  Construct a basic get ok frame from a received frame
     *
     *  @param  frame   received frame
     */
    BasicGetOKFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _deliveryTag(frame.nextUint64()),
        _redelivered(frame),
        _exchange(frame),
        _routingKey(frame),
        _messageCount(frame.nextUint32())
    {}

    /**
     *  Destructor
     */
    virtual ~BasicGetOKFrame() {}

    /**
     *  Return the name of the exchange to publish to
     *  @return string
     */
    const std::string& exchange() const
    {
        return _exchange;
    }

    /**
     *  Return the routing key
     *  @return string
     */
    const std::string& routingKey() const
    {
        return _routingKey;
    }

    /**
     *  Return the method ID
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 71;
    }

    /**
     *  Return the server-assigned and channel specific delivery tag
     *  @return uint64_t
     */
    uint64_t deliveryTag() const
    {
        return _deliveryTag;
    }

    /**
     *  Return the number of messages in the queue
     *  @return uint32_t
     */
    uint32_t messageCount() const
    {
        return _messageCount;
    }

    /**
     *  Return whether the message has been previously delivered to (another) client
     *  @return bool
     */
    bool redelivered() const
    {
        return _redelivered.get(0);
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
        if (!channel) return false;

        // report success for the get operation (this will also update the current receiver!)
        channel->reportSuccess(messageCount(), _deliveryTag, redelivered());

        // get the current receiver object
        auto *receiver = channel->receiver();

        // check if we have a valid receiver
        if (receiver == nullptr) return false;

        // initialize the receiver for the upcoming message
        receiver->initialize(_exchange, _routingKey);

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

