/**
 *  ProtocolException.h
 *
 *  This exception is thrown internally in the library when invalid data is
 *  received from the server. The best remedy is to close the connection
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
#include "exception.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class ProtocolException : public Exception
{
public:
    /**
     *  Constructor
     *  @param  what
     */
    explicit ProtocolException(const std::string &what) : Exception(what) {}
};

/**
 *  End of namespace
 */
}

