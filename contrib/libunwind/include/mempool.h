/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2003 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

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

#ifndef mempool_h
#define mempool_h

/* Memory pools provide simple memory management of fixed-size
   objects.  Memory pools are used for two purposes:

     o To ensure a stack can be unwound even when a process
       is out of memory.

      o To ensure a stack can be unwound at any time in a
        multi-threaded process (e.g., even at a time when the normal
        malloc-lock is taken, possibly by the very thread that is
        being unwind).


    To achieve the second objective, memory pools allocate memory
    directly via mmap() system call (or an equivalent facility).

    The first objective is accomplished by reserving memory ahead of
    time.  Since the memory requirements of stack unwinding generally
    depends on the complexity of the procedures being unwind, there is
    no absolute guarantee that unwinding will always work, but in
    practice, this should not be a serious problem.  */

#include <sys/types.h>

#include "libunwind_i.h"

#define sos_alloc(s)            UNWI_ARCH_OBJ(_sos_alloc)(s)
#define mempool_init(p,s,r)     UNWI_ARCH_OBJ(_mempool_init)(p,s,r)
#define mempool_alloc(p)        UNWI_ARCH_OBJ(_mempool_alloc)(p)
#define mempool_free(p,o)       UNWI_ARCH_OBJ(_mempool_free)(p,o)

/* The mempool structure should be treated as an opaque object.  It's
   declared here only to enable static allocation of mempools.  */
struct mempool
  {
    pthread_mutex_t lock;
    size_t obj_size;            /* object size (rounded up for alignment) */
    size_t chunk_size;          /* allocation granularity */
    unsigned int reserve;       /* minimum (desired) size of the free-list */
    unsigned int num_free;      /* number of objects on the free-list */
    struct object
      {
        struct object *next;
      }
    *free_list;
  };

/* Emergency allocation for one-time stuff that doesn't fit the memory
   pool model.  A limited amount of memory is available in this
   fashion and once allocated, there is no way to free it.  */
extern void *sos_alloc (size_t size);

/* Initialize POOL for an object size of OBJECT_SIZE bytes.  RESERVE
   is the number of objects that should be reserved for use under
   tight memory situations.  If it is zero, mempool attempts to pick a
   reasonable default value.  */
extern void mempool_init (struct mempool *pool,
                          size_t obj_size, size_t reserve);
extern void *mempool_alloc (struct mempool *pool);
extern void mempool_free (struct mempool *pool, void *object);

#endif /* mempool_h */
