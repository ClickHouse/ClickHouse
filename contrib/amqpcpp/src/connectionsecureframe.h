/**
 *  Class describing connection setup security challenge
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
class ConnectionSecureFrame : public ConnectionFrame
{
private:
    /**
     *  The security challenge
     *  @var LongString
     */
    LongString _challenge;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ConnectionFrame::fill(buffer);

        // encode fields
        _challenge.fill(buffer);
    }

public:
    /**
     *  Construct a connection security challenge frame
     *
     *  @param  challenge   the challenge
     */
    ConnectionSecureFrame(const std::string& challenge) :
        ConnectionFrame((uint32_t)(challenge.length() + 4)), // 4 for the length of the challenge (uint32_t)
        _challenge(challenge)
    {}

    /**
     *  Construct a connection secure frame from a received frame
     *
     *  @param  frame   received frame
     */
    ConnectionSecureFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame),
        _challenge(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionSecureFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 20;
    }

    /**
     *  Get the challenge
     *  @return string
     */
    const std::string& challenge() const 
    {
        return _challenge;
    }
};

/**
 *  end namespace
 */
}

