/**
 *  Watchable.cpp
 *
 *  @copyright 2014 Copernica BV
 */
#include "includes.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Destructor
 */
Watchable::~Watchable()
{
    // loop through all monitors
    for (auto iter = _monitors.begin(); iter != _monitors.end(); iter++)
    {
        // tell the monitor that it now is invalid
        (*iter)->invalidate();
    }
}

/**
 *  End of namespace
 */
}

