/**
 *  LibUV.h
 *
 *  Implementation for the AMQP::TcpHandler that is optimized for libuv. You can
 *  use this class instead of a AMQP::TcpHandler class, just pass the event loop
 *  to the constructor and you're all set.
 *
 *  Based heavily on the libev.h implementation by Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *
 *  @author David Nikdel <david@nikdel.com>
 *  @copyright 2015 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <uv.h>

#include "amqpcpp/linux_tcp.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class LibUvHandler : public TcpHandler
{
private:
    /**
     *  Helper class that wraps a libev I/O watcher
     */
    class Watcher
    {
    private:
        /**
         *  The event loop to which it is attached
         *  @var uv_loop_t
         */
        uv_loop_t *_loop;

        /**
         *  The actual watcher structure
         *  @var uv_poll_t
         */
        uv_poll_t *_poll;

        /**
         *  Callback method that is called by libuv when a filedescriptor becomes active
         *  @param  handle     Internal poll handle
         *  @param  status     LibUV error code UV_*
         *  @param  events     Events triggered
         */
        static void callback(uv_poll_t *handle, int status, int events)
        {
            // retrieve the connection
            TcpConnection *connection = static_cast<TcpConnection*>(handle->data);

            // tell the connection that its filedescriptor is active
            int fd = -1;
            uv_fileno(reinterpret_cast<uv_handle_t*>(handle), &fd);
            connection->process(fd, uv_to_amqp_events(status, events));
        }

    public:
        /**
         *  Constructor
         *  @param  loop            The current event loop
         *  @param  connection      The connection being watched
         *  @param  fd              The filedescriptor being watched
         *  @param  events          The events that should be monitored
         */
        Watcher(uv_loop_t *loop, TcpConnection *connection, int fd, int events) : _loop(loop)
        {
            // create a new poll
            _poll = new uv_poll_t();

            // initialize the libev structure
            uv_poll_init(_loop, _poll, fd);

            // store the connection in the data "void*"
            _poll->data = connection;

            // start the watcher
            uv_poll_start(_poll, amqp_to_uv_events(events), callback);
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
            uv_poll_stop(_poll);

            // close the handle
            uv_close(reinterpret_cast<uv_handle_t*>(_poll), [](uv_handle_t* handle) {
                // delete memory once closed
                delete reinterpret_cast<uv_poll_t*>(handle);
            });
        }

        /**
         *  Change the events for which the filedescriptor is monitored
         *  @param  events
         */
        void events(int events)
        {
            // update the events being watched for
            uv_poll_start(_poll, amqp_to_uv_events(events), callback);
        }

    private:

        /**
         *  Convert event flags from UV format to AMQP format
         */
        static int uv_to_amqp_events(int status, int events)
        {
            // if the socket is closed report both so we pick up the error
            if (status != 0)
                return AMQP::readable | AMQP::writable;

            // map read or write
            int amqp_events = 0;
            if (events & UV_READABLE)
                amqp_events |= AMQP::readable;
            if (events & UV_WRITABLE)
                amqp_events |= AMQP::writable;
            return amqp_events;
        }

        /**
         *  Convert event flags from AMQP format to UV format
         */
        static int amqp_to_uv_events(int events)
        {
            int uv_events = 0;
            if (events & AMQP::readable)
                uv_events |= UV_READABLE;
            if (events & AMQP::writable)
                uv_events |= UV_WRITABLE;
            return uv_events;
        }
    };


    /**
     *  The event loop
     *  @var uv_loop_t*
     */
    uv_loop_t *_loop;

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
            _watchers[fd] = std::unique_ptr<Watcher>(new Watcher(_loop, connection, fd, flags));
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
     *  @param  loop    The event loop to wrap
     */
    LibUvHandler(uv_loop_t *loop) : _loop(loop) {}

    /**
     *  Destructor
     */
    virtual ~LibUvHandler() = default;
};

/**
 *  End of namespace
 */
}
