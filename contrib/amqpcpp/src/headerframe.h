/**
 *  Class describing an AMQP header frame
 * 
 *  @copyright 2014, 2015 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "extframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class HeaderFrame : public ExtFrame
{
protected:
    /**
     *  Construct a header frame
     *  @param  channel     Channel ID
     *  @param  size        Payload size
     */
    HeaderFrame(uint16_t channel, uint32_t size) : ExtFrame(channel, size + 2) {}  // + size of classID (2bytes)

    /**
     *  Construct based on incoming data
     *  @param  frame       Incoming frame
     */
    HeaderFrame(ReceivedFrame &frame) : ExtFrame(frame) {}

    /**
     *  Encode a header frame to a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ExtFrame::fill(buffer);

        // add type
        buffer.add(classID());
    }

public:
    /**
     *  Destructor
     */
    virtual ~HeaderFrame() {}

    /**
     *  Get the message type
     *  @return uint8_t
     */
    virtual uint8_t type() const override
    {
        return 2;
    }

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const = 0;
};

/**
 *  end namespace
 */
}

