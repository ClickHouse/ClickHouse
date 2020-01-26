/**
 *  ExtFrame.h
 *
 *  Class describing an AMQP frame. A frame can be encoded into the AMQP
 *  wireframe format, so that it can be sent over an open socket, or it can be
 *  constructed from a buffer containing AMQP wireframe format.
 *
 *  The ExtFrame is the base class for all other frames, apart from the
 *  protocol-header-frame
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "amqpcpp/frame.h"
#include "amqpcpp/receivedframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Base frame class for all frames apart from the protocol header frame
 */
class ExtFrame : public Frame
{
protected:
    /**
     *  The AMQP channel
     *  @var    uint16_t
     */
    uint16_t _channel;

    /**
     *  The payload size of the frame
     *  @var    uint32_t
     */
    uint32_t _size;

    /**
     *  Constructor for an AMQP Frame
     *
     *  The constructor is protected as you're not supposed
     *
     *  @param  channel         channel we're working on
     *  @param  size            size of the payload
     */
    ExtFrame(uint16_t channel, uint32_t size) : _channel(channel), _size(size) {}

    /**
     *  Constructor based on a received not-yet-recognized frame
     *  @param  frame           the received frame
     */
    ExtFrame(ReceivedFrame &frame) : _channel(frame.channel()), _size(frame.payloadSize()) {}

    /**
     *  Modify the size of the frame
     *  This method is called from derived classes when properties in it are
     *  changed that force the frame to get a different size
     *  @param  change          change in size
     */
    void modifySize(int32_t change)
    {
        // change payload size
        _size += change;
    }

    /**
     *  Virtual method that must be implemented by sub classes to fill the output buffer
     *  @param  buffer
     */
    virtual void fill(OutBuffer &buffer) const override
    {
        // add type, channel id and size
        buffer.add(type());
        buffer.add(_channel);
        buffer.add(_size);
    }

public:
    /**
     *  Destruct frame
     */
    virtual ~ExtFrame() {}

    /**
     *  The channel this message was sent on
     */
    uint16_t channel() const
    {
        return _channel;
    }

    /**
     *  Size of the header
     *  @return uint32_t
     */
    uint32_t headerSize() const
    {
        // 1 byte for type, 2 bytes for channel, 4 bytes for payload
        return 7;
    }

    /**
     *  Size of the trailer
     *  @return uint32_t
     */
    uint32_t trailerSize() const
    {
        // 1 byte for end-of-frame
        return 1;
    }

    /**
     *  return the total size of the frame
     *  @return uint32_t
     */
    virtual uint32_t totalSize() const override
    {
        // payload size + size of header and trailer
        return _size + headerSize() + trailerSize();
    }

    /**
     *  The size of the payload
     *  @return uint32_t
     */
    uint32_t payloadSize() const
    {
        return _size;
    }

    /**
     *  Get the message type
     */
    virtual uint8_t type() const = 0;

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // this is an exception
        throw ProtocolException("unimplemented frame type " + std::to_string(type()));

        // unreachable
        return false;
    }
};

/**
 *  End of namespace
 */
}

