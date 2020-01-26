/**
 *  Class describing a channel close frame
 * 
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class ChannelCloseFrame : public ChannelFrame
{
private:
    /**
     *  The reply code
     *  @var int16_t
     */
    int16_t _code;

    /**
     *  The reply text
     *  @var ShortString
     */
    ShortString _text;

    /**
     *  The failing class id if applicable
     *  @note:  will be 0 if no error occured
     *  @var int16_t
     */
    int16_t _failingClass;

    /**
     *  The failing method id if applicable
     *  @note:  will be 0 if no error occured
     *  @var int16_t
     */
    int16_t _failingMethod;

protected:
    /**
     *  Encode a frame on a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ChannelFrame::fill(buffer);

        // add fields
        buffer.add(_code);
        _text.fill(buffer);
        buffer.add(_failingClass);
        buffer.add(_failingMethod);
    }
public:
    /**
     *  Construct a channel close frame
     *
     *  @param  frame   received frame
     */
    ChannelCloseFrame(ReceivedFrame &frame) :
        ChannelFrame(frame),
        _code(frame.nextInt16()),
        _text(frame),
        _failingClass(frame.nextInt16()),
        _failingMethod(frame.nextInt16())
    {}
    
    /**
     *  Construct a channel close frame
     *
     *  @param  channel         channel we're working on
     *  @param  code            reply code
     *  @param  text            reply text
     *  @param  failingClass    failing class id if applicable
     *  @param  failingMethod   failing method id if applicable
     */
    ChannelCloseFrame(uint16_t channel, uint16_t code = 0, std::string text = "", uint16_t failingClass = 0, uint16_t failingMethod = 0) :
        ChannelFrame(channel, (uint32_t)(text.length() + 7)), // sizeof code, failingclass, failingmethod (2byte + 2byte + 2byte) + text length + text length byte
        _code(code),
        _text(std::move(text)),
        _failingClass(failingClass),
        _failingMethod(failingMethod)
    {}

    /**
     *  Destructor
     */
    virtual ~ChannelCloseFrame() = default;

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 40;
    }

    /**
     *  Get the reply code
     *  @return uint16_t
     */
    uint16_t code() const
    {
        return _code;
    }

    /**
     *  Get the reply text
     *  @return string
     */
    const std::string& text() const
    {
        return _text;
    }

    /**
     *  Get the failing class id if applicable
     *  @return uint16_t
     */
    uint16_t failingClass() const
    {
        return _failingClass;
    }

    /**
     *  Get the failing method id if applicable
     *  @return uint16_t
     */
    uint16_t failingMethod() const
    {
        return _failingMethod;
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
        
        // send back an ok frame
        connection->send(ChannelCloseOKFrame(this->channel()));
        
        // what if channel doesn't exist?
        if (!channel) return false;
        
        // report to the handler
        channel->reportError(_text.value().c_str());
        
        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

