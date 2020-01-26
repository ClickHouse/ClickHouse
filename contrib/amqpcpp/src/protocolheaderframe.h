/**
 *  Class describing an AMQP protocol header frame
 *  
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up header
 */
namespace AMQP {
    
/**
 *  Clas definition
 */
class ProtocolHeaderFrame : public Frame
{
private:
    /** 
     *  Protocol name (should be null-ended string "AMQP")
     *  @var char*
     */
    const char *_protocol;

    /**
     *  The protocol major version, should be 0
     *  @var uint8_t
     */
    uint8_t _protocolIDMajor;

    /**
     *  The protocol minor version, should be 9
     *  @var uint8_t
     */
    uint8_t _protocolIDMinor;

    /**
     *  the protocol revision, should be 1
     *  @var uint8_t
     */
    uint8_t _revision;
    
    /**
     *  Encode a frame on a stringbuffer
     *
     *  @param  buffer  buffer to encode frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // there is no base, add fields
        buffer.add(_protocol, 5);
        buffer.add(_protocolIDMajor);
        buffer.add(_protocolIDMinor);
        buffer.add(_revision);
    }

public:
    /**
     *  Construct based on incoming frame
     *
     *  @param  frame   received frame to decode
     */
    ProtocolHeaderFrame(ReceivedFrame& frame) :
        _protocol(frame.nextData(5)),
        _protocolIDMajor(frame.nextUint8()),
        _protocolIDMinor(frame.nextUint8()),
        _revision(frame.nextUint8())
    {}

    /**
     *  Construct a new ProtocolHeaderFrame object
     *
     *  @param  protocolIDMajor     protocol major version, should be 0
     *  @param  protocolIDMinor     protocol minor version, should be 9
     *  @param  revision            revision of version, should be 1
     */
    ProtocolHeaderFrame(uint8_t protocolIDMajor = 0,uint8_t protocolIDMinor = 9,uint8_t revision = 1) :
        _protocol("AMQP"),
        _protocolIDMajor(protocolIDMajor),
        _protocolIDMinor(protocolIDMinor),
        _revision(revision)
    {}

    /**
     *  Destructor
     */
    virtual ~ProtocolHeaderFrame() 
    {
    }

    /**
     *  return the total size of the frame
     *  @return uint32_t
     */
    virtual uint32_t totalSize() const override
    {
        // include one byte for end of frame delimiter
        return 8;
    }

    /**
     *  return the protocol major version, should be 0
     *  @return uint8_t
     */
    uint8_t protocolIDMajor() const
    {
        return _protocolIDMajor;
    }

    /**
     *  return the protocol minor version, should be 9
     *  @return uint8_t
     */
    uint8_t protocolIDMinor() const
    {
        return _protocolIDMinor;
    }

    /**
     *  return the protocol revision, should be 1
     *  @return uint8_t
     */
    uint8_t revision() const
    {
        return _revision;
    }

    /**
     *  Is this a frame that is part of the connection setup?
     *  @return bool
     */
    virtual bool partOfHandshake() const override
    {
        return true;
    }

    /**
     *  Does this frame need an end-of-frame seperator?
     *  @return bool
     */
    virtual bool needsSeparator() const override
    { 
        return false; 
    }
};

/**
 *  End of namespace
 */
}

