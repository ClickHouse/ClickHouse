#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/** \cond
 *
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define NATS_LIBUV_INCLUDE

#include <uv.h>
#include <nats.h>

#define NATS_LIBUV_ATTACH   (1)
#define NATS_LIBUV_READ     (2)
#define NATS_LIBUV_WRITE    (3)
#define NATS_LIBUV_DETACH   (4)

struct natsLibuvEvent
{
    int             type;
    bool            add;
    natsLibuvEvent  *next;

};

struct natsLibuvEvents
{
    natsConnection  *nc;
    uv_loop_t       *loop;
    uv_poll_t       *handle;
    uv_async_t      *scheduler;
    int             events;
    natsSock        socket;
    uv_mutex_t      *lock;
    natsLibuvEvent  *head;
    natsLibuvEvent  *tail;

};

// Forward declarations
natsStatus natsLibuv_Detach(void *userData);

/** \endcond
 *
 */

static uv_once_t    uvOnce = UV_ONCE_INIT;
static uv_key_t     uvLoopThreadKey;

static void initOnce(void)
{
    if (uv_key_create(&uvLoopThreadKey) != 0)
        abort();
}

/** \defgroup libuvFunctions Libuv Adapter
 *
 *  Adapter to plug a `NATS` connection to a `libuv` event loop.
 *  @{
 */

/** \brief Initialize the adapter.
 *
 * Needs to be called once so that the adapter can initialize some state.
 */
void
natsLibuv_Init(void)
{
    uv_once(&uvOnce, initOnce);
}

/** \brief Register the event loop with the thread running `uv_run()`.
 *
 * Since `libuv` is not thread-safe, the adapter needs to know in which
 * thread `uv_run()` will run for the given `loop`. It allows the adapter
 * to schedule events so that they are executed in the event loop thread.
 *
 * @param loop an event loop.
 */
void
natsLibuv_SetThreadLocalLoop(uv_loop_t *loop)
{
    uv_key_set(&uvLoopThreadKey, static_cast<void*>(loop));
}

static natsStatus
uvScheduleToEventLoop(natsLibuvEvents *nle, int eventType, bool add)
{
    uv_mutex_lock(nle->lock);
    if (!nle->scheduler)
    {
        uv_mutex_unlock(nle->lock);
        return NATS_ILLEGAL_STATE;
    }
    uv_mutex_unlock(nle->lock);

    natsLibuvEvent  *newEvent = nullptr;
    int             res;

    newEvent = static_cast<natsLibuvEvent*>(malloc(sizeof(natsLibuvEvent)));
    if (newEvent == nullptr)
        return NATS_NO_MEMORY;

    newEvent->type  = eventType;
    newEvent->add   = add;
    newEvent->next  = nullptr;

    uv_mutex_lock(nle->lock);

    if (nle->head == nullptr)
        nle->head = newEvent;

    if (nle->tail != nullptr)
        nle->tail->next = newEvent;

    nle->tail = newEvent;

    res = uv_async_send(nle->scheduler);

    uv_mutex_unlock(nle->lock);

    return (res == 0 ? NATS_OK : NATS_ERR);
}

static void
natsLibuvPoll(uv_poll_t* handle, int status, int events)
{
    natsLibuvEvents *nle = static_cast<natsLibuvEvents*>(handle->data);

    if (status != 0)
    {
        // There was an error, try to process as a read event.
        // If we had an issue with the socket, this will cause
        // an auto-reconnect.
        natsConnection_ProcessReadEvent(nle->nc);
        return;
    }

    if (events & UV_READABLE)
        natsConnection_ProcessReadEvent(nle->nc);

    if (events & UV_WRITABLE)
        natsConnection_ProcessWriteEvent(nle->nc);
}

static natsStatus
uvPollUpdate(natsLibuvEvents *nle, int eventType, bool add)
{
    int res;

    if (eventType == NATS_LIBUV_READ)
    {
        if (add)
            nle->events |= UV_READABLE;
        else
            nle->events &= ~UV_READABLE;
    }
    else
    {
        if (add)
            nle->events |= UV_WRITABLE;
        else
            nle->events &= ~UV_WRITABLE;
    }

    if (nle->events)
        res = uv_poll_start(nle->handle, nle->events, natsLibuvPoll);
    else
        res = uv_poll_stop(nle->handle);

    if (res != 0)
        return NATS_ERR;

    return NATS_OK;
}

static void
uvHandleClosedCb(uv_handle_t *handle)
{
    free(handle);
}

static natsStatus
uvAsyncAttach(natsLibuvEvents *nle)
{
    natsStatus  s = NATS_OK;

    // We are reconnecting, destroy the old handle, create a new one
    if (nle->handle != nullptr)
    {
        uv_close(reinterpret_cast<uv_handle_t*>(nle->handle), uvHandleClosedCb);
        nle->handle = nullptr;
    }

    nle->handle = static_cast<uv_poll_t*>(malloc(sizeof(uv_poll_t)));
    if (nle->handle == nullptr)
        s = NATS_NO_MEMORY;

    if (s == NATS_OK)
    {
#if UV_VERSION_MAJOR <= 1
        if (uv_poll_init_socket(nle->loop, nle->handle, nle->socket) != 0)
#else
        if (uv_poll_init(nle->loop, nle->handle, nle->socket) != 0)
#endif
            s = NATS_ERR;
    }

    if ((s == NATS_OK)
        && (nle->handle->data = static_cast<void*>(nle))
        && (uv_poll_start(nle->handle, UV_READABLE, natsLibuvPoll) != 0))
    {
        s = NATS_ERR;
    }

    return s;
}

static void
finalCloseCb(uv_handle_t* handle)
{
    free(handle);
}

static void
closeSchedulerCb(uv_handle_t* scheduler)
{
    free(scheduler);
}

static void
uvAsyncDetach(natsLibuvEvents *nle)
{
    if (nle->lock)
        uv_mutex_lock(nle->lock);

    uv_handle_t * scheduler = reinterpret_cast<uv_handle_t*>(nle->scheduler);
    nle->scheduler = nullptr;

    uv_handle_t* handle = reinterpret_cast<uv_handle_t*>(nle->handle);
    nle->handle = nullptr;

    natsLibuvEvent  *event;
    while ((event = nle->head) != nullptr)
    {
        nle->head = event->next;
        free(event);
    }

    if (nle->lock)
    {
        uv_mutex_unlock(nle->lock);
        uv_mutex_destroy(nle->lock);

        free(nle->lock);
        nle->lock = nullptr;
    }
    free(nle);

    if (scheduler)
        uv_close(scheduler, closeSchedulerCb);

    if (handle)
        uv_close(handle, finalCloseCb);
}

static void
uvAsyncCb(uv_async_t *handle)
{
    natsLibuvEvents *nle    = static_cast<natsLibuvEvents*>(handle->data);
    natsStatus      s       = NATS_OK;
    natsLibuvEvent  *event  = nullptr;
    bool            more    = false;

    while (1)
    {
        uv_mutex_lock(nle->lock);

        event = nle->head;
        if (event == nullptr)
        {
            // This is possible, even on entry of this function because
            // the callback is called when the handle is initialized.
            uv_mutex_unlock(nle->lock);
            return;
        }

        nle->head = event->next;
        if (event == nle->tail)
            nle->tail = nullptr;

        more = (nle->head != nullptr ? true : false);

        uv_mutex_unlock(nle->lock);

        switch (event->type)
        {
            case NATS_LIBUV_ATTACH:
            {
                s = uvAsyncAttach(nle);
                break;
            }
            case NATS_LIBUV_READ:
            case NATS_LIBUV_WRITE:
            {
                s = uvPollUpdate(nle, event->type, event->add);
                break;
            }
            case NATS_LIBUV_DETACH:
            {
                uvAsyncDetach(nle);
                break;
            }
            default:
            {
                s = NATS_ERR;
                break;
            }
        }

        free(event);

        if ((s != NATS_OK) || !more)
            break;
    }

    if (s != NATS_OK)
        natsConnection_Close(nle->nc);
}

/** \brief Attach a connection to the given event loop.
 *
 * This callback is invoked after `NATS` library has connected, or reconnected.
 * For a reconnect event, `*userData` will not be `nullptr`. This function will
 * start polling on READ events for the given `socket`.
 *
 * @param userData the location where the adapter stores the user object passed
 * to the other callbacks.
 * @param loop the event loop as a generic pointer. Cast to appropriate type.
 * @param nc the connection to attach to the event loop
 * @param socket the socket to start polling on.
 */
natsStatus
natsLibuv_Attach(void **userData, void *loop, natsConnection *nc, natsSock socket)
{
    uv_loop_t       *uvLoop = static_cast<uv_loop_t*>(loop);
    bool            sched   = false;
    natsLibuvEvents *nle    = static_cast<natsLibuvEvents*>(*userData);
    natsStatus      s       = NATS_OK;

    sched = ((uv_key_get(&uvLoopThreadKey) != loop) ? true : false);

    // This is the first attach (when reconnecting, nle will be non-nullptr).
    if (nle == nullptr)
    {
        // This has to run from the event loop!
        if (sched)
            return NATS_ILLEGAL_STATE;

        nle = static_cast<natsLibuvEvents*>(calloc(1, sizeof(natsLibuvEvents)));
        if (nle == nullptr)
            return NATS_NO_MEMORY;

        nle->lock = static_cast<uv_mutex_t*>(malloc(sizeof(uv_mutex_t)));
        if (nle->lock == nullptr)
            s = NATS_NO_MEMORY;

        if ((s == NATS_OK) && (uv_mutex_init(nle->lock) != 0))
        {
            free(nle->lock);
            nle->lock = nullptr;

            s = NATS_ERR;
        }

        if ((s == NATS_OK) && ((nle->scheduler = static_cast<uv_async_t*>(malloc(sizeof(uv_async_t)))) == nullptr))
        {
            uv_mutex_destroy(nle->lock);
            free(nle->lock);
            nle->lock = nullptr;

            s = NATS_NO_MEMORY;
        }

        if ((s == NATS_OK) && (uv_async_init(uvLoop, nle->scheduler, uvAsyncCb) != 0))
        {
            uv_mutex_destroy(nle->lock);
            free(nle->lock);
            nle->lock = nullptr;

            free(nle->scheduler);
            nle->scheduler = nullptr;

            s = NATS_ERR;
        }

        if (s == NATS_OK)
        {
            nle->nc              = nc;
            nle->loop            = uvLoop;
            nle->scheduler->data = static_cast<void*>(nle);
        }
        else
        {
            free(nle);
        }
    }

    if (s == NATS_OK)
    {
        nle->socket = socket;
        nle->events = UV_READABLE;

        if (sched)
            s = uvScheduleToEventLoop(nle, NATS_LIBUV_ATTACH, true);
        else
            s = uvAsyncAttach(nle);
    }

    if (s == NATS_OK)
        *userData = static_cast<void*>(nle);

    return s;
}

/** \brief Start or stop polling on READ events.
 *
 * This callback is invoked to notify that the event library should start
 * or stop polling for READ events.
 *
 * @param userData the user object created in #natsLibuv_Attach
 * @param add `true` if the library needs to start polling, `false` otherwise.
 */
natsStatus
natsLibuv_Read(void *userData, bool add)
{
    natsLibuvEvents *nle = static_cast<natsLibuvEvents*>(userData);
    natsStatus      s    = NATS_OK;
    bool            sched;

    sched = ((uv_key_get(&uvLoopThreadKey) != nle->loop) ? true : false);

    // If this call is made from a different thread than the event loop's
    // thread, or if there are already scheduled events, then schedule
    // this new event.

    // We don't need to get the lock for nle->head because if sched is
    // false, we are in the event loop thread, which is the thread removing
    // events from the list. Also, all calls to the read/write/etc.. callbacks
    // are protected by the connection's lock in the NATS library.
    if (sched || (nle->head != nullptr))
        s = uvScheduleToEventLoop(nle, NATS_LIBUV_READ, add);
    else
        s = uvPollUpdate(nle, NATS_LIBUV_READ, add);

    return s;
}

/** \brief Start or stop polling on WRITE events.
 *
 * This callback is invoked to notify that the event library should start
 * or stop polling for WRITE events.
 *
 * @param userData the user object created in #natsLibuv_Attach
 * @param add `true` if the library needs to start polling, `false` otherwise.
 */
natsStatus
natsLibuv_Write(void *userData, bool add)
{
    natsLibuvEvents *nle = static_cast<natsLibuvEvents*>(userData);
    natsStatus      s    = NATS_OK;
    bool            sched;

    sched = ((uv_key_get(&uvLoopThreadKey) != nle->loop) ? true : false);

    // See comment in natsLibuvRead
    if (sched || (nle->head != nullptr))
        s = uvScheduleToEventLoop(nle, NATS_LIBUV_WRITE, add);
    else
        s = uvPollUpdate(nle, NATS_LIBUV_WRITE, add);

    return s;
}

/** \brief The connection is closed, it can be safely detached.
 *
 * When a connection is closed (not disconnected, pending a reconnect), this
 * callback will be invoked. This is the opportunity to cleanup the state
 * maintained by the adapter for this connection.
 *
 * @param userData the user object created in #natsLibuv_Attach
 */
natsStatus
natsLibuv_Detach(void *userData)
{
    natsLibuvEvents *nle = static_cast<natsLibuvEvents*>(userData);
    natsStatus      s    = NATS_OK;
    bool            sched;

    sched = ((uv_key_get(&uvLoopThreadKey) != nle->loop) ? true : false);

    // See comment in natsLibuvRead
    if (sched || (nle->head != nullptr))
        s = uvScheduleToEventLoop(nle, NATS_LIBUV_DETACH, true);
    else
        uvAsyncDetach(nle);

    return s;
}

/** @} */ // end of libuvFunctions

#ifdef __cplusplus
}
#endif
