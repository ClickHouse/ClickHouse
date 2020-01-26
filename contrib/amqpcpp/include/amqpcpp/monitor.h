/**
 *  Monitor.h
 *
 *  A monitor object monitors if the connection is still valid. When the
 *  connection is parsing incoming data, it calls the user handler for each
 *  incoming frame. However, it is unknown what this handler is going to do,
 *  it could for example decide to destruct the connection object. In that
 *  case the connection object should stop further handling the data. This
 *  monitor class is used to check if the connection has been destructed.
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
#include "watchable.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class Monitor
{
private:
    /**
     *  The object being watched
     *  @var    Watchable
     */
    Watchable *_watchable;

    /**
     *  Invalidate the object
     */
    void invalidate()
    {
        _watchable = nullptr;
    }

public:
    /**
     *  Constructor
     *  @param  watchable
     */
    Monitor(Watchable *watchable) : _watchable(watchable)
    {
        // register with the watchable
        _watchable->add(this);
    }

    /**
     *  Copy constructor
     *  @param  monitor
     */
    Monitor(const Monitor &monitor) : _watchable(monitor._watchable)
    {
        // register with the watchable
        if (_watchable) _watchable->add(this);
    }

    /**
     * Assignment operator
     * @param  monitor
     */
    Monitor& operator= (const Monitor &monitor)
    {
        // remove from watchable
        if (_watchable) _watchable->remove(this);

        // replace watchable
        _watchable = monitor._watchable;

        // register with the watchable
        if (_watchable) _watchable->add(this);

        return *this;
    }

    /**
     *  Destructor
     */
    virtual ~Monitor()
    {
        // remove from watchable
        if (_watchable) _watchable->remove(this);
    }

    /**
     *  Cast to boolean: is object in valid state?
     *  @return bool
     */
    operator bool () const
    {
        return _watchable != nullptr;
    }
    
    /**
     *  Negate operator: is the object in an invalid state?
     *  @return bool
     */
    bool operator! () const
    {
        return _watchable == nullptr;
    }

    /**
     *  Check if the object is valid
     *  @return bool
     */
    bool valid() const
    {
        return _watchable != nullptr;
    }

    /**
     *  The watchable can access private data
     */
    friend class Watchable;
};



/**
 *  End of namespace
 */
}



