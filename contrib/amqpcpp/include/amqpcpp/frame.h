/**
 *  Frame.h
 *
 *  Base class for frames. This base class can not be constructed from outside
 *  the library, and is only used internally.
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "protocolexception.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Forward declarations
 */
class ConnectionImpl;

/**
 *  Class definition
 */
class Frame
{
protected:
    /**
     *  Protected constructor to ensure that no objects are created from
     *  outside the library
     */
    Frame() {}

public:
    /**
     *  Destructor
     */
    virtual ~Frame() {}

    /**
     *  return the total size of the frame
     *  @return uint32_t
     */
    virtual uint32_t totalSize() const = 0;

    /**
     *  Fill an output buffer
     *  @param  buffer
     */
    virtual void fill(OutBuffer &buffer) const = 0;

    /**
     *  Is this a frame that is part of the connection setup?
     *  @return bool
     */
    virtual bool partOfHandshake() const { return false; }

    /**
     *  Is this a frame that is part of the connection close operation?
     *  @return bool
     */
    virtual bool partOfShutdown() const { return false; }

    /**
     *  Does this frame need an end-of-frame seperator?
     *  @return bool
     */
    virtual bool needsSeparator() const { return true; }

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    virtual bool synchronous() const { return false; }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection)
    {
        // make sure compilers dont complain about unused parameters
        (void) connection;

        // this is an exception
        throw ProtocolException("unimplemented frame");

        // unreachable
        return false;
    }
};

/**
 *  End of namespace
 */
}

