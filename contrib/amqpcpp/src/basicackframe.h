/**
 *  Class describing a basic acknowledgement frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class defintion
 */
class BasicAckFrame : public BasicFrame {
private:
    /**
     *  server-assigned and channel specific delivery tag
     *  @var    uint64_t
     */
    uint64_t _deliveryTag;

    /**
     *  if set, tag is treated as "up to and including", so client can acknowledge multiple messages with a single method
     *  if not set, refers to single message
     *  @var    BooleanSet
     */
    BooleanSet _multiple;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param   buffer  buffer to write frame to
     *  @return  pointer to object to allow for chaining
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        BasicFrame::fill(buffer);

        // add the delivery tag
        buffer.add(_deliveryTag);

        // add the booleans
        _multiple.fill(buffer);
    }

public:
    /**
     *  Construct a basic acknowledgement frame
     *
     *  @param  channel         Channel identifier
     *  @param  deliveryTag     server-assigned and channel specific delivery tag
     *  @param  multiple        acknowledge mutiple messages
     */
    BasicAckFrame(uint16_t channel, uint64_t deliveryTag, bool multiple = false) :
        BasicFrame(channel, 9),
        _deliveryTag(deliveryTag),
        _multiple(multiple) {}

    /**
     *  Construct based on received frame
     *  @param  frame
     */
    BasicAckFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _deliveryTag(frame.nextUint64()),
        _multiple(frame) {}

    /**
     *  Destructor
     */
    virtual ~BasicAckFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    virtual bool synchronous() const override
    {
        return false;
    }

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 80;
    }

    /**
     *  Return the server-assigned and channel specific delivery tag
     *  @return  uint64_t
     */
    uint64_t deliveryTag() const
    {
        return _deliveryTag;
    }

    /**
     *  Return whether to acknowledge multiple messages
     *  @return  bool
     */
    bool multiple() const
    {
        return _multiple.get(0);
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

        // get the current confirm
        auto confirm = channel->confirm();

        // if there is no deferred confirm, we can just as well stop
        if (confirm == nullptr) return false;

        // process the frame
        confirm->process(*this);

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

