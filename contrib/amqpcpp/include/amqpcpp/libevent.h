/**
 *  LibEvent.h
 *
 *  Implementation for the AMQP::TcpHandler that is optimized for libevent. You can
 *  use this class instead of a AMQP::TcpHandler class, just pass the event loop
 *  to the constructor and you're all set
 *
 *  Compile with: "g++ -std=c++11 libevent.cpp -lamqpcpp -levent -lpthread"
 *
 *  @author Brent Dimmig <brentdimmig@gmail.com>
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <event2/event.h>
#include <amqpcpp/flags.h>
#include <amqpcpp/linux_tcp.h>

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class LibEventHandler : public TcpHandler
{
private:
    /**
     *  Helper class that wraps a libev I/O watcher
     */
    class Watcher
    {
    private:
        /**
         *  The actual event structure
         *  @var struct event
         */
        struct event * _event;

        /**
         *  Callback method that is called by libevent when a filedescriptor becomes active
         *  @param  fd                   The filedescriptor with an event
         *  @param  what                 Events triggered
         *  @param  connection_arg       void * to the connection
         */
        static void callback(evutil_socket_t fd, short what, void *connection_arg)
        {
            // retrieve the connection
            TcpConnection *connection = static_cast<TcpConnection*>(connection_arg);

            // setup amqp flags
            int amqp_flags = 0;
            if (what & EV_READ)
                amqp_flags |= AMQP::readable;
            if (what & EV_WRITE)
                amqp_flags |= AMQP::writable;

            // tell the connection that its filedescriptor is active
            connection->process(fd, amqp_flags);
        }

    public:
        /**
         *  Constructor
         *  @param  evbase          The current event loop
         *  @param  connection      The connection being watched
         *  @param  fd              The filedescriptor being watched
         *  @param  events          The events that should be monitored
         */
        Watcher(struct event_base *evbase, TcpConnection *connection, int fd, int events)
        {
            // setup libevent flags
            short event_flags = EV_PERSIST;
            if (events & AMQP::readable)
                event_flags |= EV_READ;
            if (events & AMQP::writable)
                event_flags |= EV_WRITE;

            // initialize the event

            _event = event_new(evbase, fd, event_flags, callback, connection);
            event_add(_event, nullptr);
        }

        /**
         *  Destructor
         */
        virtual ~Watcher()
        {
            // stop the event
            event_del(_event);
            // free the event
            event_free(_event);
        }

        /**
         *  Change the events for which the filedescriptor is monitored
         *  @param  events
         */
        void events(int events)
        {
            // stop the event if it was active
            event_del(_event);

            // setup libevent flags
            short event_flags = EV_PERSIST;
            if (events & AMQP::readable)
                event_flags |= EV_READ;
            if (events & AMQP::writable)
                event_flags |= EV_WRITE;

            // set the events
            event_assign(_event, event_get_base(_event), event_get_fd(_event), event_flags,
                         event_get_callback(_event), event_get_callback_arg(_event));

            // and restart it
            event_add(_event, nullptr);
        }
    };


    /**
     *  The event loop
     *  @var struct event_base*
     */
    struct event_base *_evbase;

    /**
     *  All I/O watchers that are active, indexed by their filedescriptor
     *  @var std::map<int,Watcher>
     */
    std::map<int,std::unique_ptr<Watcher>> _watchers;


    /**
     *  Method that is called by AMQP-CPP to register a filedescriptor for readability or writability
     *  @param  connection  The TCP connection object that is reporting
     *  @param  fd          The filedescriptor to be monitored
     *  @param  flags       Should the object be monitored for readability or writability?
     */
    virtual void monitor(TcpConnection *connection, int fd, int flags) override
    {
        // do we already have this filedescriptor
        auto iter = _watchers.find(fd);

        // was it found?
        if (iter == _watchers.end())
        {
            // we did not yet have this watcher - but that is ok if no filedescriptor was registered
            if (flags == 0) return;

            // construct a new watcher, and put it in the map
            _watchers[fd] = std::unique_ptr<Watcher>(new Watcher(_evbase, connection, fd, flags));
        }
        else if (flags == 0)
        {
            // the watcher does already exist, but we no longer have to watch this watcher
            _watchers.erase(iter);
        }
        else
        {
            // change the events
            iter->second->events(flags);
        }
    }

public:
    /**
     *  Constructor
     *  @param  evbase  The event loop to wrap
     */
    LibEventHandler(struct event_base *evbase) : _evbase(evbase) {}

    /**
     *  Destructor
     */
    virtual ~LibEventHandler() = default;
};

/**
 *  End of namespace
 */
}
