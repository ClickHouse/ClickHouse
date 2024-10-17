#pragma once

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

struct NATSLibuvEvent
{
    int                     type;
    bool                    add;
    struct NATSLibuvEvent  *next;

};

struct NATSLibuvEvents
{
    natsConnection  *nc;
    uv_loop_t       *loop;
    uv_poll_t       *handle;
    uv_async_t      *scheduler;
    int             events;
    natsSock        socket;
    uv_mutex_t      *lock;
    NATSLibuvEvent  *head;
    NATSLibuvEvent  *tail;

};

void natsLibuvInit();

void natsLibuvSetThreadLocalLoop(uv_loop_t *loop);

natsStatus natsLibuvAttach(void **userData, void *loop, natsConnection *nc, natsSock socket);

natsStatus natsLibuvRead(void *userData, bool add);

natsStatus natsLibuvWrite(void *userData, bool add);

natsStatus natsLibuvDetach(void *userData);
