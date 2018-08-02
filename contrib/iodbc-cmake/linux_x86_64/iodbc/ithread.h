#pragma once

#include <pthread.h>

#define IODBC_THREADING

#define THREAD_IDENT ((unsigned long)(pthread_self()))

# define MUTEX_DECLARE(M)       pthread_mutex_t M

/// iodbc is an extremely bad library.
///
/// Here we apply a patch to make mutex recursive, because otherwise
/// it deadlocks even in simpliest usage scenario when we pass a non-existent DSN.
///
/// #1 in __GI___pthread_mutex_lock (mutex=0x87a93e0 <iodbcdm_global_lock>) at ../nptl/pthread_mutex_lock.c:78
/// #2 in SQLGetInfo (...) at ../contrib/iodbc/iodbc/info.c:1108
/// #3 in SQLGetDiagField_Internal (...) at ../contrib/iodbc/iodbc/herr.c:1464
/// #4 in SQLGetDiagFieldA (...) at ../contrib/iodbc/iodbc/herr.c:1914

#undef PTHREAD_MUTEX_INITIALIZER

# define MUTEX_INIT(M) \
    do \
    { \
        pthread_mutexattr_t attr; \
        pthread_mutexattr_init(&attr); \
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE); \
        pthread_mutex_init(&M, &attr); \
    } while (0);

# define MUTEX_DONE(M)          pthread_mutex_destroy(&M)
# define MUTEX_LOCK(M)          pthread_mutex_lock(&M)
# define MUTEX_UNLOCK(M)        pthread_mutex_unlock(&M)

# define SPINLOCK_DECLARE(M)    MUTEX_DECLARE(M)
# define SPINLOCK_INIT(M)       MUTEX_INIT(M)
# define SPINLOCK_DONE(M)       MUTEX_DONE(M)
# define SPINLOCK_LOCK(M)       MUTEX_LOCK(M)
# define SPINLOCK_UNLOCK(M)     MUTEX_UNLOCK(M)
