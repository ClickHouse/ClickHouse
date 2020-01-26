/**
 *  Class describing a basic recover frame
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
class BasicRecoverFrame : public BasicFrame {
private:
    /**
     *  Server will try to requeue messages. If requeue is false or requeue attempt fails, messages are discarded or dead-lettered
     *  @var    BooleanSet
     */
    BooleanSet _requeue;

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

        // encode fields
        _requeue.fill(buffer);
    }

public:
    /**
     *  Construct a basic recover frame from a received frame
     *
     *  @param  frame   received frame
     */
    BasicRecoverFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _requeue(frame)
    {}

    /**
     *  Construct a basic recover frame
     *
     *  @param  channel         channel ID
     *  @param  requeue         whether to attempt to requeue messages
     */
    BasicRecoverFrame(uint16_t channel, bool requeue = false) :
        BasicFrame(channel, 1), //sizeof bool
        _requeue(requeue)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicRecoverFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    bool synchronous() const override
    {
        return false;
    }

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 110;
    }

    /**
     *  Return whether the server will try to requeue
     *  @return bool
     */
    bool requeue() const
    {
        return _requeue.get(0);
    }

};

/**
 *  end namespace
 */
}

