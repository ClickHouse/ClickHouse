/**
 *  Class describing an AMQP basic frame
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
#include "methodframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 * Class implementation
 */
class BasicFrame : public MethodFrame
{
protected:
    /**
     *  Constructor
     *  @param  channel     The channel ID
     *  @param  size        Payload size
     */
    BasicFrame(uint16_t channel, uint32_t size) : MethodFrame(channel, size) {}

    /**
     *  Constructor based on a received frame
     *  @param  frame
     */
    BasicFrame(ReceivedFrame &frame) : MethodFrame(frame) {}


public:
    /**
     *  Destructor
     */
    virtual ~BasicFrame() {}

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const override
    {
        return 60;
    }
};

/**
 *  end namespace
 */
}

