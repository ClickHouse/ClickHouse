/*-
 * Copyright (c) 1998 Alex Nash
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $FreeBSD: src/lib/libc_r/uthread/uthread_rwlock.c,v 1.6 2001/04/10 04:19:20 deischen Exp $
 */

/* Replacement for macOS pthread_rwlock, which permanently deadlocks (loses wakeups)
 * when waiting threads receive signals at a high rate, e.g. from the sampling query
 * profiler. Darwin's psynch rwlock retries an interrupted kernel wait with the
 * sequence snapshot captured before the first sleep (`_pthread_rwlock_lock_wait` in
 * libpthread `src/pthread_rwlock.c`, unchanged since libpthread-454.40.3), so a
 * waiter that takes EINTR while the lock generation moves on re-enters a wait that
 * is never signaled again, and the lock wedges with no holder.
 * See https://github.com/ClickHouse/ClickHouse/issues/111579 (reported to Apple as
 * FB24027930).
 *
 * These strong definitions override libpthread's for every call from code linked
 * into ClickHouse binaries (all contribs are linked statically: libfiu, OpenSSL,
 * Poco, RocksDB, kj, ...). Calls made inside Apple's dylibs keep binding to the
 * real libpthread via the two-level namespace, which is intended: we must not
 * replace locks we do not own. If a rwlock ever crossed that boundary (created by
 * a dylib, used by us), the signature check below aborts loudly instead of
 * corrupting it.
 *
 * The implementation is the classic mutex + two condition variables read-write
 * lock by Alex Nash, taken from FreeBSD libc_r (BSD-2-Clause, see above), which
 * Apple's own pthread_rwlock was based on before the psynch rewrite in Mac OS X
 * 10.6 introduced the bug. Condition variables tolerate spurious wakeups, so
 * signal interruption has no correctness role here. Donor file (last revision,
 * including the 2004 recursive-rdlock workaround, removed from FreeBSD only
 * because libc_r itself was retired in 2010):
 * https://github.com/freebsd/freebsd-src/blob/d79326ecc875fe7885e8b19012c3c1fcf8ad89d3/lib/libc_r/uthread/uthread_rwlock.c
 *
 * Adaptations to the donor:
 *  - FreeBSD's pthread_rwlock_t is a pointer to a malloc'd struct; ours must be
 *    the caller's pthread_rwlock_t itself, so the state lives inside its 192
 *    opaque bytes, and locks born from PTHREAD_RWLOCK_INITIALIZER (which never
 *    call init) are initialized lazily via an atomic signature word.
 *  - libc_r's per-thread `curthread->rdlock_count` (the recursive-rdlock
 *    deadlock workaround) becomes a C11 thread-local; current FreeBSD libthr
 *    still uses the same counter, paired with the same decrement on unlock.
 *  - libc_r internals (`_pthread_*` names, `_get_curthread`) become the public
 *    pthread API; the waits run on Darwin's psynch mutex/condvar, which handle
 *    signal interruption correctly (condvars may simply wake spuriously).
 *
 * Not supported, by design: process-shared rwlocks (EINVAL at init; nothing we
 * link uses them, and an in-binary override could not work across processes)
 * and thread cancellation while blocked inside these locks (ClickHouse never
 * calls pthread_cancel; the donor had the same property).
 */

#if defined(__APPLE__)

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <unistd.h>

#define MAX_READ_LOCKS (INT_MAX - 1)

typedef struct
{
    pthread_mutex_t lock;
    pthread_cond_t read_signal;
    pthread_cond_t write_signal;
    int state; /* > 0 read locks held, -1 write lock held, 0 free */
    int blocked_writers;
} shim_rwlock;

typedef struct
{
    _Atomic long sig;
    shim_rwlock impl;
} shim_rwlock_storage;

/* The shim state must fit into the caller's pthread_rwlock_t, whose layout is
 * { long __sig; char __opaque[192]; } on 64-bit Darwin. */
_Static_assert(sizeof(shim_rwlock_storage) <= sizeof(pthread_rwlock_t), "shim state must fit into pthread_rwlock_t");
_Static_assert(_Alignof(shim_rwlock_storage) <= _Alignof(pthread_rwlock_t), "shim state must not need stricter alignment");
/* The sig word must overlay Apple's __sig field, so PTHREAD_RWLOCK_INITIALIZER
 * (which sets __sig to _PTHREAD_RWLOCK_SIG_init) is visible through it. */
_Static_assert(offsetof(shim_rwlock_storage, sig) == 0, "sig must be the first field");

/* Set by PTHREAD_RWLOCK_INITIALIZER (a Darwin ABI constant, stable since it is
 * compiled into every binary that ever used the static initializer). */
#define APPLE_SIG_STATIC_INIT 0x2DA8B3B4L
_Static_assert(APPLE_SIG_STATIC_INIT == _PTHREAD_RWLOCK_SIG_init, "PTHREAD_RWLOCK_INITIALIZER signature changed");

/* 'ShRW' / the same with the last letter changed: states of the shim sig word. */
#define SHIM_SIG_READY 0x53685257L
#define SHIM_SIG_INITIALIZING 0x53685249L
#define SHIM_SIG_DESTROYED 0x53685244L

static void abortWithMessage(const char * message)
{
    size_t len = strlen(message);
    ssize_t unused = write(STDERR_FILENO, message, len);
    (void)unused;
    abort();
}

/* The recursive-rdlock deadlock workaround from the donor: if this thread holds
 * any read locks, a new rdlock bypasses writer priority, because blocking behind
 * a writer that in turn waits for this thread's earlier read lock would deadlock.
 * "I hope the reader can follow that logic ;-)" (Alex Nash / FreeBSD). */
static _Thread_local int rdlock_count = 0;

static int initImpl(shim_rwlock * impl)
{
    int ret = pthread_mutex_init(&impl->lock, NULL);
    if (ret != 0)
        return ret;

    ret = pthread_cond_init(&impl->read_signal, NULL);
    if (ret != 0)
    {
        pthread_mutex_destroy(&impl->lock);
        return ret;
    }

    ret = pthread_cond_init(&impl->write_signal, NULL);
    if (ret != 0)
    {
        pthread_cond_destroy(&impl->read_signal);
        pthread_mutex_destroy(&impl->lock);
        return ret;
    }

    impl->state = 0;
    impl->blocked_writers = 0;
    return 0;
}

/* Returns the ready-to-use shim, lazily initializing storage that came from
 * PTHREAD_RWLOCK_INITIALIZER. Anything else (a lock initialized by the real
 * libpthread in a dylib, a destroyed lock, garbage) aborts: silently returning
 * an error would let lock-free execution continue unnoticed, since callers
 * like libfiu do not check the return code. */
static shim_rwlock * ensureReady(pthread_rwlock_t * rwlock)
{
    shim_rwlock_storage * storage = (shim_rwlock_storage *)rwlock;

    for (;;)
    {
        long sig = atomic_load_explicit(&storage->sig, memory_order_acquire);
        if (sig == SHIM_SIG_READY)
            return &storage->impl;

        if (sig == APPLE_SIG_STATIC_INIT)
        {
            if (atomic_compare_exchange_strong_explicit(
                    &storage->sig, &sig, SHIM_SIG_INITIALIZING, memory_order_acq_rel, memory_order_acquire))
            {
                if (initImpl(&storage->impl) != 0)
                    abortWithMessage("pthread_rwlock shim: lazy initialization failed\n");
                atomic_store_explicit(&storage->sig, SHIM_SIG_READY, memory_order_release);
                return &storage->impl;
            }
            /* Lost the race, re-read the sig. */
        }
        else if (sig == SHIM_SIG_INITIALIZING)
            sched_yield();
        else
            abortWithMessage("pthread_rwlock shim: rwlock is destroyed, foreign or corrupted\n");
    }
}

int pthread_rwlock_init(pthread_rwlock_t * rwlock, const pthread_rwlockattr_t * attr)
{
    if (rwlock == NULL)
        return EINVAL;

    if (attr != NULL)
    {
        int pshared = PTHREAD_PROCESS_PRIVATE;
        if (pthread_rwlockattr_getpshared(attr, &pshared) == 0 && pshared != PTHREAD_PROCESS_PRIVATE)
            return EINVAL; /* see the header comment: process-shared is unsupported */
    }

    shim_rwlock_storage * storage = (shim_rwlock_storage *)rwlock;
    int ret = initImpl(&storage->impl);
    if (ret != 0)
        return ret;

    atomic_store_explicit(&storage->sig, SHIM_SIG_READY, memory_order_release);
    return 0;
}

int pthread_rwlock_destroy(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock_storage * storage = (shim_rwlock_storage *)rwlock;
    long sig = atomic_load_explicit(&storage->sig, memory_order_acquire);

    /* Statically initialized and never used: nothing was ever created. */
    if (sig == APPLE_SIG_STATIC_INIT)
    {
        atomic_store_explicit(&storage->sig, SHIM_SIG_DESTROYED, memory_order_release);
        return 0;
    }

    if (sig != SHIM_SIG_READY)
        abortWithMessage("pthread_rwlock shim: destroying a rwlock that is not initialized\n");

    shim_rwlock * impl = &storage->impl;

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;
    if (impl->state != 0 || impl->blocked_writers != 0)
    {
        pthread_mutex_unlock(&impl->lock);
        return EBUSY;
    }
    pthread_mutex_unlock(&impl->lock);

    pthread_cond_destroy(&impl->read_signal);
    pthread_cond_destroy(&impl->write_signal);
    pthread_mutex_destroy(&impl->lock);
    atomic_store_explicit(&storage->sig, SHIM_SIG_DESTROYED, memory_order_release);
    return 0;
}

int pthread_rwlock_rdlock(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock * impl = ensureReady(rwlock);

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;

    if (impl->state == MAX_READ_LOCKS)
    {
        pthread_mutex_unlock(&impl->lock);
        return EAGAIN;
    }

    if (rdlock_count > 0 && impl->state > 0)
    {
        /* Recursive rdlock: bypass writer priority (see rdlock_count above). */
    }
    else
    {
        /* Give writers priority over readers (matches the Darwin man page). */
        while (impl->blocked_writers || impl->state < 0)
        {
            ret = pthread_cond_wait(&impl->read_signal, &impl->lock);
            if (ret != 0)
            {
                pthread_mutex_unlock(&impl->lock);
                return ret;
            }
        }
    }

    ++rdlock_count;
    ++impl->state;

    pthread_mutex_unlock(&impl->lock);
    return 0;
}

int pthread_rwlock_tryrdlock(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock * impl = ensureReady(rwlock);

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;

    if (impl->state == MAX_READ_LOCKS)
        ret = EAGAIN;
    else if (rdlock_count > 0 && impl->state > 0)
    {
        /* Recursive rdlock: bypass writer priority (see rdlock_count above). */
        ++rdlock_count;
        ++impl->state;
    }
    else if (impl->blocked_writers || impl->state < 0)
        ret = EBUSY;
    else
    {
        ++rdlock_count;
        ++impl->state;
    }

    pthread_mutex_unlock(&impl->lock);
    return ret;
}

int pthread_rwlock_wrlock(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock * impl = ensureReady(rwlock);

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;

    while (impl->state != 0)
    {
        ++impl->blocked_writers;
        ret = pthread_cond_wait(&impl->write_signal, &impl->lock);
        --impl->blocked_writers;
        if (ret != 0)
        {
            pthread_mutex_unlock(&impl->lock);
            return ret;
        }
    }

    impl->state = -1;

    pthread_mutex_unlock(&impl->lock);
    return 0;
}

int pthread_rwlock_trywrlock(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock * impl = ensureReady(rwlock);

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;

    if (impl->state != 0)
        ret = EBUSY;
    else
        impl->state = -1;

    pthread_mutex_unlock(&impl->lock);
    return ret;
}

int pthread_rwlock_unlock(pthread_rwlock_t * rwlock)
{
    if (rwlock == NULL)
        return EINVAL;

    shim_rwlock * impl = ensureReady(rwlock);

    int ret = pthread_mutex_lock(&impl->lock);
    if (ret != 0)
        return ret;

    if (impl->state > 0)
    {
        --rdlock_count;
        --impl->state;
        if (impl->state == 0 && impl->blocked_writers)
            pthread_cond_signal(&impl->write_signal);
    }
    else if (impl->state < 0)
    {
        impl->state = 0;
        if (impl->blocked_writers)
            pthread_cond_signal(&impl->write_signal);
        else
            pthread_cond_broadcast(&impl->read_signal);
    }
    else
        ret = EINVAL;

    pthread_mutex_unlock(&impl->lock);
    return ret;
}

#endif
