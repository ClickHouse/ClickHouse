/**
 *  Exception.h
 *
 *  Base class for all AMQP exceptions
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
#include <stdexcept>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Base exception class
 */
class Exception : public std::runtime_error
{
protected:
    /**
     *  Constructor
     *  @param  what
     */
    explicit Exception(const std::string &what) : runtime_error(what) {}
};

/**
 *  End of namespace
 */
}
