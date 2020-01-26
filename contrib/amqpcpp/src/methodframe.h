/**
 *  Class describing an AMQP method frame
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
#include "extframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class MethodFrame : public ExtFrame
{
protected:
    /**
     *  Constructor for a methodFrame
     *
     *  @param  channel     channel we're working on
     *  @param  size        size of the frame.
     */
    MethodFrame(uint16_t channel, uint32_t size) : ExtFrame(channel, size + 4) {} // size of classID and methodID

    /**
     *  Load a method from from a received frame
     *  @param  frame       The received frame
     */
    MethodFrame(ReceivedFrame &frame) : ExtFrame(frame) {}

    /**
     *  Fill an output buffer
     *  @param  buffer
     */
    virtual void fill(OutBuffer &buffer) const override
    {
        // call base
        ExtFrame::fill(buffer);

        // add type
        buffer.add(classID());
        buffer.add(methodID());
    }

public:
    /**
     *  Destructor
     */
    virtual ~MethodFrame() {}

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    bool synchronous() const override { return true; }

    /**
     *  Get the message type
     *  @return uint8_t
     */
    virtual uint8_t type() const override
    {
        return 1;
    }

    /**
     *  Class id
     *  @return uint16_t
     */
    virtual uint16_t classID() const = 0;

    /**
     *  Method id
     *  @return uint16_t
     */
    virtual uint16_t methodID() const = 0;

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // this is an exception
        throw ProtocolException("unimplemented frame type " + std::to_string(type()) + " class " + std::to_string(classID()) + " method " + std::to_string(methodID()));
    }
};

/**
 *  end namespace
 */
}

