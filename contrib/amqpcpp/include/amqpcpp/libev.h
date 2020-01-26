/**
 *  LibEV.h
 *
 *  Implementation for the AMQP::TcpHandler that is optimized for libev. You can
 *  use this class instead of a AMQP::TcpHandler class, just pass the event loop
 *  to the constructor and you're all set
 *
 *  Compile with: "g++ -std=c++11 libev.cpp -lamqpcpp -lev -lpthread"
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <ev.h>
#include <list>
#include "amqpcpp/linux_tcp.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class LibEvHandler : public TcpHandler
{
private:
    /**
     *  Internal interface for the object that is being watched
     */
    class Watchable
    {
    public:
        /**
         *  The method that is called when a filedescriptor becomes active
         *  @param  fd
         *  @param  events
         */
        virtual void onActive(int fd, int events) = 0;
        
        /**
         *  Method that is called when the timer expires
         */
        virtual void onExpired() = 0;
    };

    /**
     *  Helper class that wraps a libev I/O watcher
     */
    class Watcher
    {
    private:
        /**
         *  The event loop to which it is attached
         *  @var struct ev_loop
         */
        struct ev_loop *_loop;

        /**
         *  The actual watcher structure
         *  @var struct ev_io
         */
        struct ev_io _io;

        /**
         *  Callback method that is called by libev when a filedescriptor becomes active
         *  @param  loop        The loop in which the event was triggered
         *  @param  w           Internal watcher object
         *  @param  revents     Events triggered
         */
        static void callback(struct ev_loop *loop, struct ev_io *watcher, int revents)
        {
            // retrieve the watched object
            Watchable *object = static_cast<Watchable*>(watcher->data);

            // tell the object that its filedescriptor is active
            object->onActive(watcher->fd, revents);
        }

    public:
        /**
         *  Constructor
         *  @param  loop            The current event loop
         *  @param  object          The object being watched
         *  @param  fd              The filedescriptor being watched
         *  @param  events          The events that should be monitored
         */
        Watcher(struct ev_loop *loop, Watchable *object, int fd, int events) : _loop(loop)
        {
            // initialize the libev structure
            ev_io_init(&_io, callback, fd, events);

            // store the object in the data "void*"
            _io.data = object;

            // start the watcher
            ev_io_start(_loop, &_io);
        }

        /**
         *  Watchers cannot be copied or moved
         *
         *  @param  that    The object to not move or copy
         */
        Watcher(Watcher &&that) = delete;
        Watcher(const Watcher &that) = delete;

        /**
         *  Destructor
         */
        virtual ~Watcher()
        {
            // stop the watcher
            ev_io_stop(_loop, &_io);
        }
        
        /**
         *  Check if a filedescriptor is covered by the watcher
         *  @param  fd
         *  @return bool
         */
        bool contains(int fd) const { return _io.fd == fd; }

        /**
         *  Change the events for which the filedescriptor is monitored
         *  @param  events
         */
        void events(int events)
        {
            // stop the watcher if it was active
            ev_io_stop(_loop, &_io);

            // set the events
            ev_io_set(&_io, _io.fd, events);

            // and restart it
            ev_io_start(_loop, &_io);
        }
    };

    /**
     *  Wrapper around a connection, this will monitor the filedescriptors
     *  and run a timer to send out heartbeats
     */
    class Wrapper : private Watchable
    {
    private:
        /**
         *  The connection that is monitored
         *  @var TcpConnection
         */
        TcpConnection *_connection;
    
        /**
         *  The event loop to which it is attached
         *  @var struct ev_loop
         */
        struct ev_loop *_loop;

        /**
         *  The watcher for the timer
         *  @var struct ev_io
         */
        struct ev_timer _timer;
        
        /**
         *  IO-watchers to monitor filedescriptors
         *  @var std::list
         */
        std::list<Watcher> _watchers;
        
        /**
         *  When should we send the next heartbeat?
         *  @var ev_tstamp
         */
        ev_tstamp _next = 0.0;
        
        /**
         *  When does the connection expire / was the server for a too longer period of time idle?
         *  During connection setup, this member is used as the connect-timeout.
         *  @var ev_tstamp
         */
        ev_tstamp _expire;
        
        /**
         *  Timeout after which the connection is no longer considered alive.
         *  A heartbeat must be sent every _timeout / 2 seconds.
         *  Value zero means heartbeats are disabled, or not yet negotiated.
         *  @var uint16_t
         */
        uint16_t _timeout = 0;

        /**
         *  Callback method that is called by libev when the timer expires
         *  @param  loop        The loop in which the event was triggered
         *  @param  timer       Internal timer object
         *  @param  revents     The events that triggered this call
         */
        static void callback(struct ev_loop *loop, struct ev_timer *timer, int revents)
        {
            // retrieve the object
            Watchable *object = static_cast<Watchable*>(timer->data);

            // tell the object that it expired
            object->onExpired();
        }
        
        /**
         *  Do we need timers / is this a timed monitor?
         *  @return bool
         */
        bool timed() const
        {
            // if neither timers are set
            return _expire > 0.0 || _next > 0.0;
        }
        
        /**
         *  Method that is called when the timer expired
         */
        virtual void onExpired() override
        {
            // get the current time
            ev_tstamp now = ev_now(_loop);
            
            // timer is no longer active, so the refcounter in the loop is restored
            ev_ref(_loop);
            
            // if the onNegotiate method was not yet called, and no heartbeat timeout was negotiated
            if (_timeout == 0)
            {
                // this can happen in three situations: 1. a connect-timeout, 2. user space has
                // told us that we're not interested in heartbeats, 3. rabbitmq does not want heartbeats,
                // in either case we're no longer going to run further timers.
                _next = _expire = 0.0;
                
                // if we have a valid connection, user-space must have overridden the onNegotiate
                // method, so we keep using the connection
                if (_connection->ready()) return;
                
                // this is a connection timeout, close the connection from our side too
                return (void)_connection->close(true);
            }
            else if (now >= _expire)
            {
                // the server was inactive for a too long period of time, reset state
                _next = _expire = 0.0; _timeout = 0;
                
                // close the connection because server was inactive
                return (void)_connection->close();
            }
            else if (now >= _next)
            {
                // it's time for the next heartbeat
                _connection->heartbeat();
                
                // remember when we should send out the next one, so the next one should be 
                // sent only after _timout/2 seconds again _from now_ (no catching up)
                _next = now + std::max(_timeout / 2, 1);
            }
            
            // reset the timer to trigger again later
            _timer.repeat = std::min(_next, _expire) - now;
            
            // restart the timer
            ev_timer_again(_loop, &_timer);
            
            // and because the timer is running again, we restore the refcounter
            ev_unref(_loop);
        }
        
        /**
         *  Method that is called when a filedescriptor becomes active
         *  @param  fd          the filedescriptor that is active
         *  @param  events      the events that are active (readable/writable)
         */
        virtual void onActive(int fd, int events) override
        {
            // if the server is readable, we have some extra time before it expires, the expire time 
            // is set to 1.5 * _timeout to close the connection when the third heartbeat is about to be sent
            if (_timeout != 0 && (events & EV_READ)) _expire = ev_now(_loop) + _timeout * 1.5;
            
            // pass on to the connection
            _connection->process(fd, events);
        }

    public:
        /**
         *  Constructor
         *  @param  loop            The current event loop
         *  @param  connection      The TCP connection
         *  @param  timeout         Connect timeout
         */
        Wrapper(struct ev_loop *loop, AMQP::TcpConnection *connection, uint16_t timeout = 60) : 
            _connection(connection),
            _loop(loop),
            _next(0.0),
            _expire(ev_now(loop) + timeout),
            _timeout(0)
        {
            // store the object in the data "void*"
            _timer.data = this;
            
            // initialize the libev structure, it should expire after the connection timeout
            ev_timer_init(&_timer, callback, timeout, 0.0);

            // start the timer (this is the time that we reserve for setting up the connection)
            ev_timer_start(_loop, &_timer);

            // the timer should not keep the event loop active
            ev_unref(_loop);
        }

        /**
         *  Watchers cannot be copied or moved
         *
         *  @param  that    The object to not move or copy
         */
        Wrapper(Wrapper &&that) = delete;
        Wrapper(const Wrapper &that) = delete;

        /**
         *  Destructor
         */
        virtual ~Wrapper()
        {
            // the timer was already stopped
            if (!timed()) return;

            // stop the timer
            ev_timer_stop(_loop, &_timer);

            // restore loop refcount
            ev_ref(_loop);
        }

        /**
         *  Start the timer (and expose the interval)
         *  @param  interval        the heartbeat interval proposed by the server
         *  @return uint16_t        the heartbeat interval that we accepted
         */
        uint16_t start(uint16_t timeout)
        {
            // we now know for sure that the connection was set up
            _timeout = timeout;
            
            // if heartbeats are disabled we do not have to set it
            if (_timeout == 0) return 0;
            
            // calculate current time
            auto now = ev_now(_loop);
            
            // we also know when the next heartbeat should be sent
            _next = now + std::max(_timeout / 2, 1);
            
            // because the server has just sent us some data, we will update the expire time too
            _expire = now + _timeout * 1.5;

            // find the earliest thing that expires
            _timer.repeat = std::min(_next, _expire) - now;
            
            // restart the timer
            ev_timer_again(_loop, &_timer);
            
            // expose the accepted interval
            return _timeout;
        }
        
        /**
         *  Check if the timer is associated with a certain connection
         *  @param  connection
         *  @return bool
         */
        bool contains(const AMQP::TcpConnection *connection) const
        {
            // compare the connections
            return _connection == connection;
        }
        
        /**
         *  Monitor a filedescriptor
         *  @param  fd
         *  @param  events
         */
        void monitor(int fd, int events)
        {
            // should we remove?
            if (events == 0)
            {
                // remove the io-watcher
                _watchers.remove_if([fd](const Watcher &watcher) -> bool {
                    
                    // compare filedescriptors
                    return watcher.contains(fd);
                });
            }
            else
            {
                // look in the array for this filedescriptor
                for (auto &watcher : _watchers)
                {
                    // do we have a match?
                    if (watcher.contains(fd)) return watcher.events(events);
                }
                
                // we need a watcher
                Watchable *watchable = this;
                
                // we should monitor a new filedescriptor
                _watchers.emplace_back(_loop, watchable, fd, events);
            }
        }
    };

    /**
     *  The event loop
     *  @var struct ev_loop*
     */
    struct ev_loop *_loop;
    
    /**
     *  Each connection is wrapped
     *  @var std::list
     */
    std::list<Wrapper> _wrappers;

    /**
     *  Lookup a connection-wrapper, when the wrapper is not found, we construct one
     *  @param  connection
     *  @return Wrapper
     */
    Wrapper &lookup(TcpConnection *connection)
    {
        // look for the appropriate connection
        for (auto &wrapper : _wrappers)
        {
            // do we have a match?
            if (wrapper.contains(connection)) return wrapper;
        }
        
        // add to the wrappers
        _wrappers.emplace_back(_loop, connection);
        
        // done
        return _wrappers.back();
    }

    /**
     *  Method that is called by AMQP-CPP to register a filedescriptor for readability or writability
     *  @param  connection  The TCP connection object that is reporting
     *  @param  fd          The filedescriptor to be monitored
     *  @param  flags       Should the object be monitored for readability or writability?
     */
    virtual void monitor(TcpConnection *connection, int fd, int flags) override final
    {
        // lookup the appropriate wrapper and start monitoring
        lookup(connection).monitor(fd, flags);
    }

protected:
    /**
     *  Method that is called when the heartbeat timeout is negotiated between the server and the client. 
     *  @param  connection      The connection that suggested a heartbeat timeout
     *  @param  timeout         The suggested timeout from the server
     *  @return uint16_t        The timeout to use
     */
    virtual uint16_t onNegotiate(TcpConnection *connection, uint16_t timeout) override
    {
        // lookup the wrapper, and start the timer to check for activity and send heartbeats
        return lookup(connection).start(timeout);
    }

    /**
     *  Method that is called when the TCP connection is destructed
     *  @param  connection  The TCP connection
     */
    virtual void onDetached(TcpConnection *connection) override
    {
        // remove from the array
        _wrappers.remove_if([connection](const Wrapper &wrapper) -> bool {
            return wrapper.contains(connection);
        });
    }

public:
    /**
     *  Constructor
     *  @param  loop    The event loop to wrap
     */
    LibEvHandler(struct ev_loop *loop) : _loop(loop) {}

    /**
     *  Destructor
     */
    virtual ~LibEvHandler() = default;
};

/**
 *  End of namespace
 */
}
