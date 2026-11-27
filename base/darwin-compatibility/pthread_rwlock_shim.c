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

/// ClickHouse: replacement for the macOS pthread_rwlock, which permanently deadlocks (loses
/// wakeups) when waiting threads receive signals at a high rate, e.g. from the sampling query
/// profiler. Darwin's psynch rwlock retries an interrupted kernel wait with the sequence snapshot
/// captured before the first sleep (`_pthread_rwlock_lock_wait` in libpthread
/// `src/pthread_rwlock.c`, unchanged since libpthread-454.40.3), so a waiter that takes EINTR
/// while the lock generation moves on re-enters a wait that is never signaled again, and the lock
/// wedges with no holder. See https://github.com/ClickHouse/ClickHouse/issues/111579 (reported to
/// Apple as FB24027930).
///
/// ClickHouse: these strong definitions override libpthread's for every call from code linked
/// into ClickHouse binaries (all contribs are linked statically: libfiu, OpenSSL, Poco, RocksDB,
/// kj, ...). Calls made inside Apple's dylibs keep binding to the real libpthread via the
/// two-level namespace, which is intended: we must not replace locks we do not own.
///
/// ClickHouse: the implementation below is the read-write lock from FreeBSD libc_r, which Apple's
/// own pthread_rwlock was based on before the psynch rewrite in Mac OS X 10.6 introduced the bug.
/// It blocks on a mutex and condition variables, and condition variables tolerate spurious
/// wakeups, so signal interruption has no correctness role. The donor file (last revision,
/// removed from FreeBSD only because libc_r itself was retired in 2010) is
/// https://github.com/freebsd/freebsd-src/blob/d79326ecc875fe7885e8b19012c3c1fcf8ad89d3/lib/libc_r/uthread/uthread_rwlock.c
/// and its code and comments below are kept verbatim, in the donor's formatting, so that a diff
/// against it stays readable. Only these mechanical substitutions were applied:
///   - `pthread_rwlock_t prwlock` (a pointer to a malloc'd struct in the donor) becomes
///     `shim_rwlock *prwlock` stored inside the caller's pthread_rwlock_t, because the caller
///     owns that storage; consequently `init_static` becomes `ensureReady`, and destroy marks
///     the storage instead of calling free.
///   - `curthread->rdlock_count` becomes a thread-local of the same name. Current FreeBSD libthr
///     still keeps this counter, with the same comment and the same pairing on unlock.
///   - the donor's internal `_pthread_*` entry points become the public pthread API, and the
///     definitions here are named `pthread_rwlock_*` so they interpose.
///   - `MAX_READ_LOCKS` came from the donor's private header pthread_private.h.
///
/// ClickHouse: not supported, by design: process-shared rwlocks (EINVAL at init; nothing we link
/// uses them, and an in-binary override could not work across processes) and thread cancellation
/// while blocked inside these locks (ClickHouse never calls pthread_cancel; the donor had the
/// same property).

#if defined(__APPLE__)

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <unistd.h>

#define MAX_READ_LOCKS (INT_MAX - 1)

/// ClickHouse: the donor's `struct pthread_rwlock`.
typedef struct
{
	pthread_mutex_t	lock;	/* monitor lock */
	pthread_cond_t	read_signal;
	pthread_cond_t	write_signal;
	int		state;	/* 0 = idle  >0 = # of readers  -1 = writer */
	int		blocked_writers;
} shim_rwlock;

/// ClickHouse: the donor allocated the struct above and stored a pointer to it in the caller's
/// pthread_rwlock_t. We cannot: locks born from PTHREAD_RWLOCK_INITIALIZER never call init, so
/// there is no allocation point. The state therefore lives inside the caller's storage, behind a
/// signature word that overlays Apple's `__sig` field and tells the three cases apart.
typedef struct
{
	_Atomic long sig;
	shim_rwlock impl;
} shim_rwlock_storage;

_Static_assert(sizeof(shim_rwlock_storage) <= sizeof(pthread_rwlock_t), "shim state must fit into pthread_rwlock_t");
_Static_assert(_Alignof(shim_rwlock_storage) <= _Alignof(pthread_rwlock_t), "shim state must not need stricter alignment");
_Static_assert(offsetof(shim_rwlock_storage, sig) == 0, "sig must overlay Apple's __sig field");

/// ClickHouse: set by PTHREAD_RWLOCK_INITIALIZER (a Darwin ABI constant, compiled into every
/// binary that ever used the static initializer).
#define APPLE_SIG_STATIC_INIT 0x2DA8B3B4L
_Static_assert(APPLE_SIG_STATIC_INIT == _PTHREAD_RWLOCK_SIG_init, "PTHREAD_RWLOCK_INITIALIZER signature changed");

/// ClickHouse: states of the shim signature word.
#define SHIM_SIG_READY 0x53685257L
#define SHIM_SIG_INITIALIZING 0x53685249L
#define SHIM_SIG_DESTROYED 0x53685244L

/// ClickHouse: see the comment on `curthread->rdlock_count` in pthread_rwlock_rdlock below.
static _Thread_local int rdlock_count = 0;

static void abortWithMessage(const char * message)
{
	size_t len = strlen(message);
	ssize_t unused = write(STDERR_FILENO, message, len);
	(void)unused;
	abort();
}

static int initImpl(shim_rwlock *prwlock);

/// ClickHouse: replaces the donor's `init_static`. Returns the ready-to-use lock, initializing
/// storage that came from PTHREAD_RWLOCK_INITIALIZER on first use. Anything else (a lock
/// initialized by the real libpthread inside a dylib, a destroyed lock, garbage) aborts:
/// returning an error would let callers that do not check it, such as libfiu, run lock-free.
static shim_rwlock *ensureReady(pthread_rwlock_t *rwlock)
{
	shim_rwlock_storage *storage = (shim_rwlock_storage *)rwlock;

	for (;;) {
		long sig = atomic_load_explicit(&storage->sig, memory_order_acquire);
		if (sig == SHIM_SIG_READY)
			return &storage->impl;

		if (sig == APPLE_SIG_STATIC_INIT) {
			if (atomic_compare_exchange_strong_explicit(
					&storage->sig, &sig, SHIM_SIG_INITIALIZING,
					memory_order_acq_rel, memory_order_acquire)) {
				if (initImpl(&storage->impl) != 0)
					abortWithMessage("pthread_rwlock shim: lazy initialization failed\n");
				atomic_store_explicit(&storage->sig, SHIM_SIG_READY, memory_order_release);
				return &storage->impl;
			}
			/* lost the race, re-read the signature */
		} else if (sig == SHIM_SIG_INITIALIZING)
			sched_yield();
		else
			abortWithMessage("pthread_rwlock shim: rwlock is destroyed, foreign or corrupted\n");
	}
}

/// ClickHouse: the body of the donor's _pthread_rwlock_init, minus the allocation.
static int
initImpl(shim_rwlock *prwlock)
{
	int ret;

	/* initialize the lock */
	if ((ret = pthread_mutex_init(&prwlock->lock, NULL)) != 0)
		return (ret);
	else {
		/* initialize the read condition signal */
		ret = pthread_cond_init(&prwlock->read_signal, NULL);

		if (ret != 0) {
			pthread_mutex_destroy(&prwlock->lock);
		} else {
			/* initialize the write condition signal */
			ret = pthread_cond_init(&prwlock->write_signal, NULL);

			if (ret != 0) {
				pthread_cond_destroy(&prwlock->read_signal);
				pthread_mutex_destroy(&prwlock->lock);
			} else {
				/* success */
				prwlock->state = 0;
				prwlock->blocked_writers = 0;
			}
		}
	}

	return (ret);
}

int
pthread_rwlock_init (pthread_rwlock_t *rwlock, const pthread_rwlockattr_t *attr)
{
	shim_rwlock_storage *storage;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: process-shared rwlocks are not supported, see the notes at the top.
	if (attr != NULL) {
		int pshared = PTHREAD_PROCESS_PRIVATE;
		if (pthread_rwlockattr_getpshared(attr, &pshared) == 0 &&
		    pshared != PTHREAD_PROCESS_PRIVATE)
			return(EINVAL);
	}

	storage = (shim_rwlock_storage *)rwlock;

	if ((ret = initImpl(&storage->impl)) != 0)
		return (ret);

	atomic_store_explicit(&storage->sig, SHIM_SIG_READY, memory_order_release);

	return (ret);
}

int
pthread_rwlock_destroy (pthread_rwlock_t *rwlock)
{
	int ret;

	if (rwlock == NULL)
		ret = EINVAL;
	else {
		shim_rwlock_storage *storage = (shim_rwlock_storage *)rwlock;
		long sig = atomic_load_explicit(&storage->sig, memory_order_acquire);
		shim_rwlock *prwlock;

		/// ClickHouse: statically initialized and never used, so nothing was ever created.
		/// The donor would dereference a NULL pointer here.
		if (sig == APPLE_SIG_STATIC_INIT) {
			atomic_store_explicit(&storage->sig, SHIM_SIG_DESTROYED, memory_order_release);
			return (0);
		}

		if (sig != SHIM_SIG_READY)
			abortWithMessage("pthread_rwlock shim: destroying a rwlock that is not initialized\n");

		prwlock = &storage->impl;

		pthread_mutex_destroy(&prwlock->lock);
		pthread_cond_destroy(&prwlock->read_signal);
		pthread_cond_destroy(&prwlock->write_signal);

		/// ClickHouse: the donor freed the object and set *rwlock to NULL here.
		atomic_store_explicit(&storage->sig, SHIM_SIG_DESTROYED, memory_order_release);

		ret = 0;
	}
	return (ret);
}

int
pthread_rwlock_rdlock (pthread_rwlock_t *rwlock)
{
	shim_rwlock *prwlock;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: replaces the donor's `prwlock = *rwlock` and its static initialization check.
	prwlock = ensureReady(rwlock);

	/* grab the monitor lock */
	if ((ret = pthread_mutex_lock(&prwlock->lock)) != 0)
		return(ret);

	/* check lock count */
	if (prwlock->state == MAX_READ_LOCKS) {
		pthread_mutex_unlock(&prwlock->lock);
		return (EAGAIN);
	}

	if ((rdlock_count > 0) && (prwlock->state > 0)) {
		/*
		 * To avoid having to track all the rdlocks held by
		 * a thread or all of the threads that hold a rdlock,
		 * we keep a simple count of all the rdlocks held by
		 * a thread.  If a thread holds any rdlocks it is
		 * possible that it is attempting to take a recursive
		 * rdlock.  If there are blocked writers and precedence
		 * is given to them, then that would result in the thread
		 * deadlocking.  So allowing a thread to take the rdlock
		 * when it already has one or more rdlocks avoids the
		 * deadlock.  I hope the reader can follow that logic ;-)
		 */
		;	/* nothing needed */
	} else {
		/* give writers priority over readers */
		while (prwlock->blocked_writers || prwlock->state < 0) {
			ret = pthread_cond_wait(&prwlock->read_signal,
			    &prwlock->lock);

			if (ret != 0) {
				/* can't do a whole lot if this fails */
				pthread_mutex_unlock(&prwlock->lock);
				return(ret);
			}
		}
	}

	rdlock_count++;
	prwlock->state++; /* indicate we are locked for reading */

	/*
	 * Something is really wrong if this call fails.  Returning
	 * error won't do because we've already obtained the read
	 * lock.  Decrementing 'state' is no good because we probably
	 * don't have the monitor lock.
	 */
	pthread_mutex_unlock(&prwlock->lock);

	return (ret);
}

int
pthread_rwlock_tryrdlock (pthread_rwlock_t *rwlock)
{
	shim_rwlock *prwlock;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: replaces the donor's `prwlock = *rwlock` and its static initialization check.
	prwlock = ensureReady(rwlock);

	/* grab the monitor lock */
	if ((ret = pthread_mutex_lock(&prwlock->lock)) != 0)
		return(ret);

	if (prwlock->state == MAX_READ_LOCKS)
		ret = EAGAIN; /* too many read locks acquired */
	else if ((rdlock_count > 0) && (prwlock->state > 0)) {
		/* see comment for pthread_rwlock_rdlock() */
		rdlock_count++;
		prwlock->state++;
	}
	/* give writers priority over readers */
	else if (prwlock->blocked_writers || prwlock->state < 0)
		ret = EBUSY;
	else {
		prwlock->state++; /* indicate we are locked for reading */
		rdlock_count++;
	}

	/* see the comment on this in pthread_rwlock_rdlock */
	pthread_mutex_unlock(&prwlock->lock);

	return (ret);
}

int
pthread_rwlock_trywrlock (pthread_rwlock_t *rwlock)
{
	shim_rwlock *prwlock;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: replaces the donor's `prwlock = *rwlock` and its static initialization check.
	prwlock = ensureReady(rwlock);

	/* grab the monitor lock */
	if ((ret = pthread_mutex_lock(&prwlock->lock)) != 0)
		return(ret);

	if (prwlock->state != 0)
		ret = EBUSY;
	else
		/* indicate we are locked for writing */
		prwlock->state = -1;

	/* see the comment on this in pthread_rwlock_rdlock */
	pthread_mutex_unlock(&prwlock->lock);

	return (ret);
}

int
pthread_rwlock_unlock (pthread_rwlock_t *rwlock)
{
	shim_rwlock *prwlock;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: replaces the donor's `prwlock = *rwlock` and its NULL check.
	prwlock = ensureReady(rwlock);

	/* grab the monitor lock */
	if ((ret = pthread_mutex_lock(&prwlock->lock)) != 0)
		return(ret);

	if (prwlock->state > 0) {
		rdlock_count--;
		prwlock->state--;
		if (prwlock->state == 0 && prwlock->blocked_writers)
			ret = pthread_cond_signal(&prwlock->write_signal);
	} else if (prwlock->state < 0) {
		prwlock->state = 0;

		if (prwlock->blocked_writers)
			ret = pthread_cond_signal(&prwlock->write_signal);
		else
			ret = pthread_cond_broadcast(&prwlock->read_signal);
	} else
		ret = EINVAL;

	/* see the comment on this in pthread_rwlock_rdlock */
	pthread_mutex_unlock(&prwlock->lock);

	return (ret);
}

int
pthread_rwlock_wrlock (pthread_rwlock_t *rwlock)
{
	shim_rwlock *prwlock;
	int ret;

	if (rwlock == NULL)
		return(EINVAL);

	/// ClickHouse: replaces the donor's `prwlock = *rwlock` and its static initialization check.
	prwlock = ensureReady(rwlock);

	/* grab the monitor lock */
	if ((ret = pthread_mutex_lock(&prwlock->lock)) != 0)
		return(ret);

	while (prwlock->state != 0) {
		prwlock->blocked_writers++;

		ret = pthread_cond_wait(&prwlock->write_signal,
		    &prwlock->lock);

		if (ret != 0) {
			prwlock->blocked_writers--;
			pthread_mutex_unlock(&prwlock->lock);
			return(ret);
		}

		prwlock->blocked_writers--;
	}

	/* indicate we are locked for writing */
	prwlock->state = -1;

	/* see the comment on this in pthread_rwlock_rdlock */
	pthread_mutex_unlock(&prwlock->lock);

	return (ret);
}

#endif
