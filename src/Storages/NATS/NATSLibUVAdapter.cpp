/**
*  Patched adapter from NATS io library to plug a `NATS` connection to a `libuv` event loop.
 */

#include <Storages/NATS/NATSLibUVAdapter.h>

static uv_once_t    uvOnce = UV_ONCE_INIT;
static uv_key_t     uvLoopThreadKey;

static void initOnce()
{
    if (uv_key_create(&uvLoopThreadKey) != 0)
        abort();
}

void natsLibuvInit()
{
    uv_once(&uvOnce, initOnce);
}

void natsLibuvSetThreadLocalLoop(uv_loop_t *loop)
{
    uv_key_set(&uvLoopThreadKey, static_cast<void*>(loop));
}

/// Patched uvScheduleToEventLoop function from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
static natsStatus uvScheduleToEventLoop(NATSLibuvEvents *nle, int eventType, bool add)
{
    NATSLibuvEvent  *newEvent = nullptr;
    int             res;

    uv_mutex_lock(nle->lock);
    if (!nle->scheduler)
    {
        uv_mutex_unlock(nle->lock);
        return NATS_ILLEGAL_STATE;
    }
    uv_mutex_unlock(nle->lock);

    newEvent = static_cast<NATSLibuvEvent*>(malloc(sizeof(NATSLibuvEvent)));
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

static void natsLibuvPoll(uv_poll_t* handle, int status, int events)
{
    NATSLibuvEvents *nle = static_cast<NATSLibuvEvents*>(handle->data);

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

static natsStatus uvPollUpdate(NATSLibuvEvents *nle, int eventType, bool add)
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

static void uvHandleClosedCb(uv_handle_t *handle)
{
    free(handle);
}

/// Pathed uvAsyncAttach function from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
static natsStatus uvAsyncAttach(NATSLibuvEvents *nle)
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

/// Pathed finalCloseCb function from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
static void finalCloseCb(uv_handle_t* handle)
{
    free(handle);
}

/// Pathed closeSchedulerCb function from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
static void closeSchedulerCb(uv_handle_t* scheduler)
{
    free(scheduler);
}

static void uvAsyncDetach(NATSLibuvEvents *nle)
{
    if (nle->lock)
        uv_mutex_lock(nle->lock);

    uv_handle_t * scheduler = reinterpret_cast<uv_handle_t*>(nle->scheduler);
    nle->scheduler = nullptr;

    uv_handle_t* handle = reinterpret_cast<uv_handle_t*>(nle->handle);
    nle->handle = nullptr;

    NATSLibuvEvent *event;
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

static void uvAsyncCb(uv_async_t *handle)
{
    NATSLibuvEvents  *nle    = static_cast<NATSLibuvEvents*>(handle->data);
    natsStatus       s       = NATS_OK;
    NATSLibuvEvent   *event  = nullptr;
    bool             more    = false;

    while (true)
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

/// Pathed natsLibuv_Attach function from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
natsStatus natsLibuvAttach(void **userData, void *loop, natsConnection *nc, natsSock socket)
{
    uv_loop_t       *uvLoop = static_cast<uv_loop_t*>(loop);
    bool            sched   = false;
    NATSLibuvEvents *nle    = static_cast<NATSLibuvEvents*>(*userData);
    natsStatus      s       = NATS_OK;

    sched = ((uv_key_get(&uvLoopThreadKey) != loop) ? true : false);

    // This is the first attach (when reconnecting, nle will be non-nullptrptr).
    if (nle == nullptr)
    {
        // This has to run from the event loop!
        if (sched)
            return NATS_ILLEGAL_STATE;

        nle = static_cast<NATSLibuvEvents*>(calloc(1, sizeof(NATSLibuvEvents)));
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

natsStatus natsLibuvRead(void *userData, bool add)
{
    NATSLibuvEvents *nle = static_cast<NATSLibuvEvents*>(userData);
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

natsStatus natsLibuvWrite(void *userData, bool add)
{
    NATSLibuvEvents *nle = static_cast<NATSLibuvEvents*>(userData);
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

/// Pathed function natsLibuvDetach from NATS io libuv adapter (contrib/nats-io/src/adapters/libuv.h)
natsStatus natsLibuvDetach(void *userData)
{
    NATSLibuvEvents *nle = static_cast<NATSLibuvEvents*>(userData);
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
