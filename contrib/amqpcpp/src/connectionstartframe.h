/**
 *  Class describing initial connection setup
 * 
 *  This frame is sent by the server to the client, right after the connection
 *  is opened. It contains the initial connection properties, and the protocol
 *  number.
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
class ConnectionStartFrame : public ConnectionFrame
{
private:
    /**
     *  Major AMQP version number
     *  @var uint8_t
     */
    uint8_t _major;

    /**
     *  Minor AMQP version number
     *  @var uint8_t
     */
    uint8_t _minor;

    /**
     *  Additional server properties
     *  @note:  exact properties are not specified
     *          and are implementation-dependent
     *  @var Table
     */
    Table _properties;
    
    /**
     *  Available security mechanisms
     *  @var LongString
     */
    LongString _mechanisms;

    /**
     *  Available message locales
     *  @var LongString
     */
    LongString _locales;

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

        // encode all fields
        buffer.add(_major);
        buffer.add(_minor);
        _properties.fill(buffer);
        _mechanisms.fill(buffer);
        _locales.fill(buffer);
    }

public:
    /**
     *  Client-side constructer for a connection start frame
     *
     *  @param  major       major protocol version
     *  @param  minor       minor protocol version
     *  @param  properties  server properties
     *  @param  mechanisms  available security mechanisms
     *  @param  locales     available locales
     */
    ConnectionStartFrame(uint8_t major, uint8_t minor, const Table& properties, const std::string& mechanisms, const std::string& locales) :
        ConnectionFrame((uint32_t)(properties.size() + mechanisms.length() + locales.length() + 10)), // 4 for each longstring (size-uint32), 2 major/minor
        _major(major),
        _minor(minor),
        _properties(properties),
        _mechanisms(mechanisms),
        _locales(locales)
    {}

    /**
     *  Construct a connection start frame from a received frame
     *
     *  @param  frame   received frame
     */
    ConnectionStartFrame(ReceivedFrame &frame) :
        ConnectionFrame(frame),
        _major(frame.nextUint8()),
        _minor(frame.nextUint8()),
        _properties(frame),
        _mechanisms(frame),
        _locales(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~ConnectionStartFrame() {}

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 10;
    }

    /**
     *  Major AMQP version number
     *  @return uint8_t
     */
    uint8_t majorVersion() const
    {
        return _major;
    }

    /**
     *  Minor AMQP version number
     *  @return uint8_t
     */
    uint8_t minorVersion() const
    {
        return _minor;
    }

    /**
     *  Additional server properties
     *  @note:  exact properties are not specified
     *          and are implementation-dependent
     * 
     *  @return Table
     */
    const Table& properties() const
    {
        return _properties;
    }

    /**
     *  The capabilities of the connection
     *  @return Table
     */
    const Table& capabilities() const
    {
        // retrieve the capabilities
        return _properties.get("capabilities");
    }

    /**
     *  Available security mechanisms
     *  @return string
     */
    const std::string &mechanisms() const
    {
        return _mechanisms;
    }

    /**
     *  Available message locales
     *  @return string
     */
    const std::string &locales() const
    {
        return _locales;
    }
    
    /**
     *  Process the connection start frame
     *  @param  connection
     *  @return bool
     *  @internal
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // the client properties
        Table properties;

        // move connection to handshake mode
        connection->setProtocolOk(_properties, properties);

        // the capabilities
        Table capabilities;
        
        // we want a special treatment for authentication failures
        capabilities["authentication_failure_close"] = true;
        
        // fill the peer properties
        if (!properties.contains("product")) properties["product"] = "Copernica AMQP library";
        if (!properties.contains("version")) properties["version"] = "Unknown";
        if (!properties.contains("platform")) properties["platform"] = "Unknown";
        if (!properties.contains("copyright")) properties["copyright"] = "Copyright 2015 - 2018 Copernica BV";
        if (!properties.contains("information")) properties["information"] = "https://www.copernica.com";
        if (!properties.contains("capabilities")) properties["capabilities"] = capabilities;
        
        // send back a connection start ok frame
        connection->send(ConnectionStartOKFrame(properties, "PLAIN", connection->login().saslPlain(), "en_US"));
        
        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

