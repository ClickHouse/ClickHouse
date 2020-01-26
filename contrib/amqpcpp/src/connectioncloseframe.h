/**
 *  Class describing connection close frame
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
class ConnectionCloseFrame : public ConnectionFrame
{
private:
    /**
     *  The reply code
     *  @var uint16_t
     */
    uint16_t _code;

    /**
     *  The reply text
     *  @var ShortString
     */
    ShortString _text;

    /**
     *  Class id for failing class, if applicable
     *  Will be 0 in absence of errors
     *  @var uint16_t
     */
    uint16_t _failingClass;

    /**
     *  Method id for failinv class, if applicable
     *  Will be 0 in absence of errors
     *  @var uint16_t
     */
    uint16_t _failingMethod;

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

        // add fields
        buffer.add(_code);
        _text.fill(buffer);
        buffer.add(_failingClass);
        buffer.add(_failingMethod);
    }

public:
    /**
     *  Construct a connection close frame from a received frame
     *
     *  @param frame    received frame
     */
    ConnectionCloseFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame),
        _code(frame.nextUint16()),
        _text(frame),
        _failingClass(frame.nextUint16()),
        _failingMethod(frame.nextUint16())
    {}

    /**
     *  Construct a connection close frame
     *
     *  @param  code            the reply code
     *  @param  text            the reply text
     *  @param  failingClass    id of the failing class if applicable
     *  @param  failingMethod   id of the failing method if applicable
     */
    ConnectionCloseFrame(uint16_t code, std::string text, uint16_t failingClass = 0, uint16_t failingMethod = 0) :
        ConnectionFrame((uint32_t)(text.length() + 7)), // 1 for extra string byte, 2 for each uint16
        _code(code),
        _text(std::move(text)),
        _failingClass(failingClass),
        _failingMethod(failingMethod)
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionCloseFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 50;
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
     *  This frame is part of the shutdown operation
     *  @return bool
     */
    virtual bool partOfShutdown() const override
    {
        return true;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // we need the monitor because the connection could be destructed in the meantime
        Monitor monitor(connection);
        
        // send back the ok frame
        connection->send(ConnectionCloseOKFrame());
        
        // check if connection still exists
        if (!monitor.valid()) return false;
        
        // no need to check for a channel, the error is connection wide
        // report the error on the connection
        connection->reportError(text().c_str());
        
        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

