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
#include "nats.h"

#define NATS_LIBUV_ATTACH   (1)
#define NATS_LIBUV_READ     (2)
#define NATS_LIBUV_WRITE    (3)
#define NATS_LIBUV_DETACH   (4)

struct natsLibuvEvent_;

typedef struct natsLibuvEvent_
{
    int                     type;
    bool                    add;
    struct natsLibuvEvent_  *next;

} natsLibuvEvent;

typedef struct
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

} natsLibuvEvents;

/** \endcond
 *
 */

/** \defgroup libuvFunctions Libuv Adapter
 *
 *  Adapter to plug a `NATS` connection to a `libuv` event loop.
 *  @{
 */

/** \brief Initialize the adapter.
 *
 * Needs to be called once so that the adapter can initialize some state.
 */
void natsLibuv_Init(void);

/** \brief Register the event loop with the thread running `uv_run()`.
 *
 * Since `libuv` is not thread-safe, the adapter needs to know in which
 * thread `uv_run()` will run for the given `loop`. It allows the adapter
 * to schedule events so that they are executed in the event loop thread.
 *
 * @param loop an event loop.
 */
void natsLibuv_SetThreadLocalLoop(uv_loop_t *loop);

/** \brief Attach a connection to the given event loop.
 *
 * This callback is invoked after `NATS` library has connected, or reconnected.
 * For a reconnect event, `*userData` will not be `NULL`. This function will
 * start polling on READ events for the given `socket`.
 *
 * @param userData the location where the adapter stores the user object passed
 * to the other callbacks.
 * @param loop the event loop as a generic pointer. Cast to appropriate type.
 * @param nc the connection to attach to the event loop
 * @param socket the socket to start polling on.
 */
natsStatus natsLibuv_Attach(void **userData, void *loop, natsConnection *nc, natsSock socket);

/** \brief Start or stop polling on READ events.
 *
 * This callback is invoked to notify that the event library should start
 * or stop polling for READ events.
 *
 * @param userData the user object created in #natsLibuv_Attach
 * @param add `true` if the library needs to start polling, `false` otherwise.
 */
natsStatus natsLibuv_Read(void *userData, bool add);

/** \brief Start or stop polling on WRITE events.
 *
 * This callback is invoked to notify that the event library should start
 * or stop polling for WRITE events.
 *
 * @param userData the user object created in #natsLibuv_Attach
 * @param add `true` if the library needs to start polling, `false` otherwise.
 */
natsStatus natsLibuv_Write(void *userData, bool add);

/** \brief The connection is closed, it can be safely detached.
 *
 * When a connection is closed (not disconnected, pending a reconnect), this
 * callback will be invoked. This is the opportunity to cleanup the state
 * maintained by the adapter for this connection.
 *
 * @param userData the user object created in #natsLibuv_Attach
 */
natsStatus natsLibuv_Detach(void *userData);

/** @} */ // end of libuvFunctions

#ifdef __cplusplus
}
#endif
