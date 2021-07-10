/* Common threading primitives definitions for both POSIX and C11.
   Copyright (C) 2017-2018 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _THREAD_SHARED_TYPES_H
#define _THREAD_SHARED_TYPES_H 1

/* Arch-specific definitions.  Each architecture must define the following
   macros to define the expected sizes of pthread data types:

   __SIZEOF_PTHREAD_ATTR_T        - size of pthread_attr_t.
   __SIZEOF_PTHREAD_MUTEX_T       - size of pthread_mutex_t.
   __SIZEOF_PTHREAD_MUTEXATTR_T   - size of pthread_mutexattr_t.
   __SIZEOF_PTHREAD_COND_T        - size of pthread_cond_t.
   __SIZEOF_PTHREAD_CONDATTR_T    - size of pthread_condattr_t.
   __SIZEOF_PTHREAD_RWLOCK_T      - size of pthread_rwlock_t.
   __SIZEOF_PTHREAD_RWLOCKATTR_T  - size of pthread_rwlockattr_t.
   __SIZEOF_PTHREAD_BARRIER_T     - size of pthread_barrier_t.
   __SIZEOF_PTHREAD_BARRIERATTR_T - size of pthread_barrierattr_t.

   Also, the following macros must be define for internal pthread_mutex_t
   struct definitions (struct __pthread_mutex_s):

   __PTHREAD_COMPAT_PADDING_MID   - any additional members after 'kind'
				    and before '__spin' (for 64 bits) or
				    '__nusers' (for 32 bits).
   __PTHREAD_COMPAT_PADDING_END   - any additional members at the end of
				    the internal structure.
   __PTHREAD_MUTEX_LOCK_ELISION   - 1 if the architecture supports lock
				    elision or 0 otherwise.
   __PTHREAD_MUTEX_NUSERS_AFTER_KIND - control where to put __nusers.  The
				       preferred value for new architectures
				       is 0.
   __PTHREAD_MUTEX_USE_UNION      - control whether internal __spins and
				    __list will be place inside a union for
				    linuxthreads compatibility.
				    The preferred value for new architectures
				    is 0.

   For a new port the preferred values for the required defines are:

   #define __PTHREAD_COMPAT_PADDING_MID
   #define __PTHREAD_COMPAT_PADDING_END
   #define __PTHREAD_MUTEX_LOCK_ELISION         0
   #define __PTHREAD_MUTEX_NUSERS_AFTER_KIND    0
   #define __PTHREAD_MUTEX_USE_UNION            0

   __PTHREAD_MUTEX_LOCK_ELISION can be set to 1 if the hardware plans to
   eventually support lock elision using transactional memory.

   The additional macro defines any constraint for the lock alignment
   inside the thread structures:

   __LOCK_ALIGNMENT - for internal lock/futex usage.

   Same idea but for the once locking primitive:

   __ONCE_ALIGNMENT - for pthread_once_t/once_flag definition.

   And finally the internal pthread_rwlock_t (struct __pthread_rwlock_arch_t)
   must be defined.
 */
#include <bits/pthreadtypes-arch.h>

/* Common definition of pthread_mutex_t. */

#if !__PTHREAD_MUTEX_USE_UNION
typedef struct __pthread_internal_list
{
  struct __pthread_internal_list *__prev;
  struct __pthread_internal_list *__next;
} __pthread_list_t;
#else
typedef struct __pthread_internal_slist
{
  struct __pthread_internal_slist *__next;
} __pthread_slist_t;
#endif

/* Lock elision support.  */
#if __PTHREAD_MUTEX_LOCK_ELISION
# if !__PTHREAD_MUTEX_USE_UNION
#  define __PTHREAD_SPINS_DATA	\
  short __spins;		\
  short __elision
#  define __PTHREAD_SPINS             0, 0
# else
#  define __PTHREAD_SPINS_DATA	\
  struct			\
  {				\
    short __espins;		\
    short __eelision;		\
  } __elision_data
#  define __PTHREAD_SPINS         { 0, 0 }
#  define __spins __elision_data.__espins
#  define __elision __elision_data.__eelision
# endif
#else
# define __PTHREAD_SPINS_DATA int __spins
/* Mutex __spins initializer used by PTHREAD_MUTEX_INITIALIZER.  */
# define __PTHREAD_SPINS 0
#endif

struct __pthread_mutex_s
{
  int __lock __LOCK_ALIGNMENT;
  unsigned int __count;
  int __owner;
#if !__PTHREAD_MUTEX_NUSERS_AFTER_KIND
  unsigned int __nusers;
#endif
  /* KIND must stay at this position in the structure to maintain
     binary compatibility with static initializers.

     Concurrency notes:
     The __kind of a mutex is initialized either by the static
     PTHREAD_MUTEX_INITIALIZER or by a call to pthread_mutex_init.

     After a mutex has been initialized, the __kind of a mutex is usually not
     changed.  BUT it can be set to -1 in pthread_mutex_destroy or elision can
     be enabled.  This is done concurrently in the pthread_mutex_*lock functions
     by using the macro FORCE_ELISION. This macro is only defined for
     architectures which supports lock elision.

     For elision, there are the flags PTHREAD_MUTEX_ELISION_NP and
     PTHREAD_MUTEX_NO_ELISION_NP which can be set in addition to the already set
     type of a mutex.
     Before a mutex is initialized, only PTHREAD_MUTEX_NO_ELISION_NP can be set
     with pthread_mutexattr_settype.
     After a mutex has been initialized, the functions pthread_mutex_*lock can
     enable elision - if the mutex-type and the machine supports it - by setting
     the flag PTHREAD_MUTEX_ELISION_NP. This is done concurrently. Afterwards
     the lock / unlock functions are using specific elision code-paths.  */
  int __kind;
  __PTHREAD_COMPAT_PADDING_MID
#if __PTHREAD_MUTEX_NUSERS_AFTER_KIND
  unsigned int __nusers;
#endif
#if !__PTHREAD_MUTEX_USE_UNION
  __PTHREAD_SPINS_DATA;
  __pthread_list_t __list;
# define __PTHREAD_MUTEX_HAVE_PREV      1
#else
  __extension__ union
  {
    __PTHREAD_SPINS_DATA;
    __pthread_slist_t __list;
  };
# define __PTHREAD_MUTEX_HAVE_PREV      0
#endif
  __PTHREAD_COMPAT_PADDING_END
};


/* Common definition of pthread_cond_t. */

struct __pthread_cond_s
{
  __extension__ union
  {
    __extension__ unsigned long long int __wseq;
    struct
    {
      unsigned int __low;
      unsigned int __high;
    } __wseq32;
  };
  __extension__ union
  {
    __extension__ unsigned long long int __g1_start;
    struct
    {
      unsigned int __low;
      unsigned int __high;
    } __g1_start32;
  };
  unsigned int __g_refs[2] __LOCK_ALIGNMENT;
  unsigned int __g_size[2];
  unsigned int __g1_orig_size;
  unsigned int __wrefs;
  unsigned int __g_signals[2];
};

#endif /* _THREAD_SHARED_TYPES_H  */
