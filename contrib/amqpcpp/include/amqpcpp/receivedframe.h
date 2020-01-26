/**
 *  ReceivedFrame.h
 *
 *  The received frame class is a wrapper around a data buffer, it tries to
 *  find out if the buffer is big enough to contain an entire frame, and
 *  it will try to recognize the frame type in the buffer
 *
 *  This is a class that is used internally by the AMQP library. As a user
 *  of this library, you normally do not have to instantiate it.
 *
 *  @copyright 2014 - 2017 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <cstdint>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class Buffer;
class ConnectionImpl;

/**
 *  Class definition
 */
class ReceivedFrame
{
private:
    /**
     *  The buffer we are reading from
     *  @var    Buffer
     */
    const Buffer &_buffer;

    /**
     *  Number of bytes already processed
     *  @var    uint32_t
     */
    uint32_t _skip = 0;

    /**
     *  Type of frame
     *  @var    uint8_t
     */
    uint8_t _type = 0;

    /**
     *  Channel identifier
     *  @var    uint16_t
     */
    uint16_t _channel = 0;

    /**
     *  The payload size
     *  @var    uint32_t
     */
    uint32_t _payloadSize = 0;


    /**
     *  Process a method frame
     *  @param  connection
     *  @return bool
     */
    bool processMethodFrame(ConnectionImpl *connection);

    /**
     *  Process a connection frame
     *  @param  connection
     *  @return bool
     */
    bool processConnectionFrame(ConnectionImpl *connection);

    /**
     *  Process a channel frame
     *  @param  connection
     *  @return bool
     */
    bool processChannelFrame(ConnectionImpl *connection);

    /**
     *  Process an exchange frame
     *  @param  connection
     *  @return bool
     */
    bool processExchangeFrame(ConnectionImpl *connection);

    /**
     *  Process a queue frame
     *  @param  connection
     *  @return bool
     */
    bool processQueueFrame(ConnectionImpl *connection);

    /**
     *  Process a basic frame
     *  @param  connection
     *  @return bool
     */
    bool processBasicFrame(ConnectionImpl *connection);

    /**
     *  Process a confirm frame
     *  @param  connection
     *  @return bool
     */
    bool processConfirmFrame(ConnectionImpl *connection);

    /**
     *  Process a transaction frame
     *  @param  connection
     *  @return bool
     */
    bool processTransactionFrame(ConnectionImpl *connection);

    /**
     *  Process a header frame
     *  @param  connection
     *  @return bool
     */
    bool processHeaderFrame(ConnectionImpl *connection);


public:
    /**
     *  Constructor
     *  @param  buffer      Binary buffer
     *  @param  max         Max buffer size
     */
    ReceivedFrame(const Buffer &buffer, uint32_t max);

    /**
     *  Destructor
     */
    virtual ~ReceivedFrame() {}

    /**
     *  Have we at least received the full frame header?
     *  The header contains the frame type, the channel ID and the payload size
     *  @return bool
     */
    bool header() const;

    /**
     *  Is this a complete frame?
     *  @return bool
     */
    bool complete() const;

    /**
     *  Return the channel identifier
     *  @return uint16_t
     */
    uint16_t channel() const
    {
        return _channel;
    }

    /**
     *  Total size of the frame (headers + payload)
     *  @return uint32_t
     */
    uint64_t totalSize() const
    {
        // payload size + size of headers and end of frame byte
        return _payloadSize + 8;
    }

    /**
     *  The size of the payload
     *  @return uint32_t
     */
    uint32_t payloadSize() const
    {
        return _payloadSize;
    }

    /**
     *  Read the next uint8_t from the buffer
     *
     *  @return uint8_t         value read
     */
    uint8_t nextUint8();

    /**
     *  Read the next int8_t from the buffer
     *
     *  @return int8_t          value read
     */
    int8_t nextInt8();

    /**
     *  Read the next uint16_t from the buffer
     *
     *  @return uint16_t        value read
     */
    uint16_t nextUint16();

    /**
     *  Read the next int16_t from the buffer
     *
     *  @return int16_t     value read
     */
    int16_t nextInt16();

    /**
     *  Read the next uint32_t from the buffer
     *
     *  @return uint32_t        value read
     */
    uint32_t nextUint32();

    /**
     *  Read the next int32_t from the buffer
     *
     *  @return int32_t     value read
     */
    int32_t nextInt32();

    /**
     *  Read the next uint64_t from the buffer
     *
     *  @return uint64_t        value read
     */
    uint64_t nextUint64();

    /**
     *  Read the next int64_t from the buffer
     *
     *  @return int64_t     value read
     */
    int64_t nextInt64();

    /**
     *  Read a float from the buffer
     *
     *  @return float       float read from buffer.
     */
    float nextFloat();

    /**
     *  Read a double from the buffer
     *
     *  @return double      double read from buffer
     */
    double nextDouble();

    /**
     *  Get a pointer to the next binary buffer of a certain size
     *  @param  size
     *  @return char*
     */
    const char *nextData(uint32_t size);

    /**
     *  Process the received frame
     *
     *  If this method returns false, it means that the frame was not processed,
     *  because it was an unrecognized frame. This does not mean that the
     *  connection is now in an invalid state however.
     *
     *  @param  connection  the connection over which the data was received
     *  @return bool        was the frame fully processed
     *  @internal
     */
    bool process(ConnectionImpl *connection);


    /**
     *  The checker may access private data
     */
    friend class FrameCheck;

};

/**
 *  End of namespace
 */
}
