#ifndef PTHREAD_H
#define PTHREAD_H

#include "../../include/pthread.h"

hidden int __pthread_once(pthread_once_t *, void (*)(void));
hidden void __pthread_testcancel(void);
hidden int __pthread_setcancelstate(int, int *);
hidden int __pthread_create(pthread_t *restrict, const pthread_attr_t *restrict, void *(*)(void *), void *restrict);
hidden _Noreturn void __pthread_exit(void *);
hidden int __pthread_join(pthread_t, void **);
hidden int __pthread_mutex_lock(pthread_mutex_t *);
hidden int __pthread_mutex_trylock(pthread_mutex_t *);
hidden int __pthread_mutex_trylock_owner(pthread_mutex_t *);
hidden int __pthread_mutex_timedlock(pthread_mutex_t *restrict, const struct timespec *restrict);
hidden int __pthread_mutex_unlock(pthread_mutex_t *);
hidden int __private_cond_signal(pthread_cond_t *, int);
hidden int __pthread_cond_timedwait(pthread_cond_t *restrict, pthread_mutex_t *restrict, const struct timespec *restrict);
hidden int __pthread_key_create(pthread_key_t *, void (*)(void *));
hidden int __pthread_key_delete(pthread_key_t);
hidden int __pthread_rwlock_rdlock(pthread_rwlock_t *);
hidden int __pthread_rwlock_tryrdlock(pthread_rwlock_t *);
hidden int __pthread_rwlock_timedrdlock(pthread_rwlock_t *__restrict, const struct timespec *__restrict);
hidden int __pthread_rwlock_wrlock(pthread_rwlock_t *);
hidden int __pthread_rwlock_trywrlock(pthread_rwlock_t *);
hidden int __pthread_rwlock_timedwrlock(pthread_rwlock_t *__restrict, const struct timespec *__restrict);
hidden int __pthread_rwlock_unlock(pthread_rwlock_t *);

#endif
