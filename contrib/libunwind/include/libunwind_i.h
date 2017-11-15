/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2005 Hewlett-Packard Co
   Copyright (C) 2007 David Mosberger-Tang
        Contributed by David Mosberger-Tang <dmosberger@gmail.com>

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

/* This files contains libunwind-internal definitions which are
   subject to frequent change and are not to be exposed to
   libunwind-users.  */

#ifndef libunwind_i_h
#define libunwind_i_h

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "compiler.h"

#if defined(HAVE___THREAD) && HAVE___THREAD
#define UNWI_DEFAULT_CACHING_POLICY UNW_CACHE_PER_THREAD
#else
#define UNWI_DEFAULT_CACHING_POLICY UNW_CACHE_GLOBAL
#endif

/* Platform-independent libunwind-internal declarations.  */

#include <sys/types.h>  /* HP-UX needs this before include of pthread.h */

#include <assert.h>
#include <libunwind.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>

#if defined(HAVE_ELF_H)
# include <elf.h>
#elif defined(HAVE_SYS_ELF_H)
# include <sys/elf.h>
#else
# error Could not locate <elf.h>
#endif

#if defined(HAVE_ENDIAN_H)
# include <endian.h>
#elif defined(HAVE_SYS_ENDIAN_H)
# include <sys/endian.h>
# if defined(_LITTLE_ENDIAN) && !defined(__LITTLE_ENDIAN)
#   define __LITTLE_ENDIAN _LITTLE_ENDIAN
# endif
# if defined(_BIG_ENDIAN) && !defined(__BIG_ENDIAN)
#   define __BIG_ENDIAN _BIG_ENDIAN
# endif
# if defined(_BYTE_ORDER) && !defined(__BYTE_ORDER)
#   define __BYTE_ORDER _BYTE_ORDER
# endif
#else
# define __LITTLE_ENDIAN        1234
# define __BIG_ENDIAN           4321
# if defined(__hpux)
#   define __BYTE_ORDER __BIG_ENDIAN
# elif defined(__QNX__)
#   if defined(__BIGENDIAN__)
#     define __BYTE_ORDER __BIG_ENDIAN
#   elif defined(__LITTLEENDIAN__)
#     define __BYTE_ORDER __LITTLE_ENDIAN
#   else
#     error Host has unknown byte-order.
#   endif
# else
#   error Host has unknown byte-order.
# endif
#endif

#if defined(HAVE__BUILTIN_UNREACHABLE)
# define unreachable() __builtin_unreachable()
#else
# define unreachable() do { } while (1)
#endif

#ifdef DEBUG
# define UNW_DEBUG      1
#else
# define UNW_DEBUG      0
#endif

/* Make it easy to write thread-safe code which may or may not be
   linked against libpthread.  The macros below can be used
   unconditionally and if -lpthread is around, they'll call the
   corresponding routines otherwise, they do nothing.  */

#pragma weak pthread_mutex_init
#pragma weak pthread_mutex_lock
#pragma weak pthread_mutex_unlock

#define mutex_init(l)                                                   \
        (pthread_mutex_init != NULL ? pthread_mutex_init ((l), NULL) : 0)
#define mutex_lock(l)                                                   \
        (pthread_mutex_lock != NULL ? pthread_mutex_lock (l) : 0)
#define mutex_unlock(l)                                                 \
        (pthread_mutex_unlock != NULL ? pthread_mutex_unlock (l) : 0)

#ifdef HAVE_ATOMIC_OPS_H
# include <atomic_ops.h>
static inline int
cmpxchg_ptr (void *addr, void *old, void *new)
{
  union
    {
      void *vp;
      AO_t *aop;
    }
  u;

  u.vp = addr;
  return AO_compare_and_swap(u.aop, (AO_t) old, (AO_t) new);
}
# define fetch_and_add1(_ptr)           AO_fetch_and_add1(_ptr)
# define fetch_and_add(_ptr, value)     AO_fetch_and_add(_ptr, value)
   /* GCC 3.2.0 on HP-UX crashes on cmpxchg_ptr() */
#  if !(defined(__hpux) && __GNUC__ == 3 && __GNUC_MINOR__ == 2)
#   define HAVE_CMPXCHG
#  endif
# define HAVE_FETCH_AND_ADD
#elif defined(HAVE_SYNC_ATOMICS) || defined(HAVE_IA64INTRIN_H)
# ifdef HAVE_IA64INTRIN_H
#  include <ia64intrin.h>
# endif
static inline int
cmpxchg_ptr (void *addr, void *old, void *new)
{
  union
    {
      void *vp;
      long *vlp;
    }
  u;

  u.vp = addr;
  return __sync_bool_compare_and_swap(u.vlp, (long) old, (long) new);
}
# define fetch_and_add1(_ptr)           __sync_fetch_and_add(_ptr, 1)
# define fetch_and_add(_ptr, value)     __sync_fetch_and_add(_ptr, value)
# define HAVE_CMPXCHG
# define HAVE_FETCH_AND_ADD
#endif
#define atomic_read(ptr)        (*(ptr))

#define UNWI_OBJ(fn)      UNW_PASTE(UNW_PREFIX,UNW_PASTE(I,fn))
#define UNWI_ARCH_OBJ(fn) UNW_PASTE(UNW_PASTE(UNW_PASTE(_UI,UNW_TARGET),_), fn)

#define unwi_full_mask    UNWI_ARCH_OBJ(full_mask)

/* Type of a mask that can be used to inhibit preemption.  At the
   userlevel, preemption is caused by signals and hence sigset_t is
   appropriate.  In constrast, the Linux kernel uses "unsigned long"
   to hold the processor "flags" instead.  */
typedef sigset_t intrmask_t;

extern intrmask_t unwi_full_mask;

/* Silence compiler warnings about variables which are used only if libunwind
   is configured in a certain way */
static inline void mark_as_used(void *v UNUSED) {
}

#if defined(CONFIG_BLOCK_SIGNALS)
# define SIGPROCMASK(how, new_mask, old_mask) \
  sigprocmask((how), (new_mask), (old_mask))
#else
# define SIGPROCMASK(how, new_mask, old_mask) mark_as_used(old_mask)
#endif

/* Prefer adaptive mutexes if available */
#ifdef PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP
#define UNW_PTHREAD_MUTEX_INITIALIZER PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP
#else
#define UNW_PTHREAD_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#endif

#define define_lock(name) \
  pthread_mutex_t name = UNW_PTHREAD_MUTEX_INITIALIZER
#define lock_init(l)            mutex_init (l)
#define lock_acquire(l,m)                               \
do {                                                    \
  SIGPROCMASK (SIG_SETMASK, &unwi_full_mask, &(m));     \
  mutex_lock (l);                                       \
} while (0)
#define lock_release(l,m)                       \
do {                                            \
  mutex_unlock (l);                             \
  SIGPROCMASK (SIG_SETMASK, &(m), NULL);        \
} while (0)

#define SOS_MEMORY_SIZE 16384   /* see src/mi/mempool.c */

#ifndef MAP_ANONYMOUS
# define MAP_ANONYMOUS MAP_ANON
#endif
#define GET_MEMORY(mem, size)                                               \
do {                                                                        \
  /* Hopefully, mmap() goes straight through to a system call stub...  */   \
  mem = mmap (NULL, size, PROT_READ | PROT_WRITE,                           \
              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);                          \
  if (mem == MAP_FAILED)                                                    \
    mem = NULL;                                                             \
} while (0)

#define unwi_find_dynamic_proc_info     UNWI_OBJ(find_dynamic_proc_info)
#define unwi_extract_dynamic_proc_info  UNWI_OBJ(extract_dynamic_proc_info)
#define unwi_put_dynamic_unwind_info    UNWI_OBJ(put_dynamic_unwind_info)
#define unwi_dyn_remote_find_proc_info  UNWI_OBJ(dyn_remote_find_proc_info)
#define unwi_dyn_remote_put_unwind_info UNWI_OBJ(dyn_remote_put_unwind_info)
#define unwi_dyn_validate_cache         UNWI_OBJ(dyn_validate_cache)

extern int unwi_find_dynamic_proc_info (unw_addr_space_t as,
                                        unw_word_t ip,
                                        unw_proc_info_t *pi,
                                        int need_unwind_info, void *arg);
extern int unwi_extract_dynamic_proc_info (unw_addr_space_t as,
                                           unw_word_t ip,
                                           unw_proc_info_t *pi,
                                           unw_dyn_info_t *di,
                                           int need_unwind_info,
                                           void *arg);
extern void unwi_put_dynamic_unwind_info (unw_addr_space_t as,
                                          unw_proc_info_t *pi, void *arg);

/* These handle the remote (cross-address-space) case of accessing
   dynamic unwind info. */

extern int unwi_dyn_remote_find_proc_info (unw_addr_space_t as,
                                           unw_word_t ip,
                                           unw_proc_info_t *pi,
                                           int need_unwind_info,
                                           void *arg);
extern void unwi_dyn_remote_put_unwind_info (unw_addr_space_t as,
                                             unw_proc_info_t *pi,
                                             void *arg);
extern int unwi_dyn_validate_cache (unw_addr_space_t as, void *arg);

extern unw_dyn_info_list_t _U_dyn_info_list;
extern pthread_mutex_t _U_dyn_info_list_lock;

#if UNW_DEBUG
#define unwi_debug_level                UNWI_ARCH_OBJ(debug_level)
extern long unwi_debug_level;

# include <stdio.h>
# define Debug(level,format...)                                         \
do {                                                                    \
  if (unwi_debug_level >= level)                                        \
    {                                                                   \
      int _n = level;                                                   \
      if (_n > 16)                                                      \
        _n = 16;                                                        \
      fprintf (stderr, "%*c>%s: ", _n, ' ', __FUNCTION__);              \
      fprintf (stderr, format);                                         \
    }                                                                   \
} while (0)
# define Dprintf(format...)         fprintf (stderr, format)
#else
# define Debug(level,format...)
# define Dprintf(format...)
#endif

static ALWAYS_INLINE int
print_error (const char *string)
{
  return write (2, string, strlen (string));
}

#define mi_init         UNWI_ARCH_OBJ(mi_init)

extern void mi_init (void);     /* machine-independent initializations */
extern unw_word_t _U_dyn_info_list_addr (void);

/* This is needed/used by ELF targets only.  */

struct elf_image
  {
    void *image;                /* pointer to mmap'd image */
    size_t size;                /* (file-) size of the image */
  };

struct elf_dyn_info
  {
    struct elf_image ei;
    unw_dyn_info_t di_cache;
    unw_dyn_info_t di_debug;    /* additional table info for .debug_frame */
#if UNW_TARGET_IA64
    unw_dyn_info_t ktab;
#endif
#if UNW_TARGET_ARM
    unw_dyn_info_t di_arm;      /* additional table info for .ARM.exidx */
#endif
  };

static inline void invalidate_edi (struct elf_dyn_info *edi)
{
  if (edi->ei.image)
    munmap (edi->ei.image, edi->ei.size);
  memset (edi, 0, sizeof (*edi));
  edi->di_cache.format = -1;
  edi->di_debug.format = -1;
#if UNW_TARGET_ARM
  edi->di_arm.format = -1;
#endif
}


/* Provide a place holder for architecture to override for fast access
   to memory when known not to need to validate and know the access
   will be local to the process. A suitable override will improve
   unw_tdep_trace() performance in particular. */
#define ACCESS_MEM_FAST(ret,validate,cur,addr,to) \
  do { (ret) = dwarf_get ((cur), DWARF_MEM_LOC ((cur), (addr)), &(to)); } \
  while (0)

/* Define GNU and processor specific values for the Phdr p_type field in case
   they aren't defined by <elf.h>.  */
#ifndef PT_GNU_EH_FRAME
# define PT_GNU_EH_FRAME        0x6474e550
#endif /* !PT_GNU_EH_FRAME */
#ifndef PT_ARM_EXIDX
# define PT_ARM_EXIDX           0x70000001      /* ARM unwind segment */
#endif /* !PT_ARM_EXIDX */

#include "tdep/libunwind_i.h"

#ifndef tdep_get_func_addr
# define tdep_get_func_addr(as,addr,v)          (*(v) = addr, 0)
#endif

#ifndef DWARF_VAL_LOC
# define DWARF_IS_VAL_LOC(l)    0
# define DWARF_VAL_LOC(c,v)     DWARF_NULL_LOC
#endif

#define UNW_ALIGN(x,a) (((x)+(a)-1UL)&~((a)-1UL))

#endif /* libunwind_i_h */
