/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2003, 2005 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>

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

#include "libunwind_i.h"

/* From GCC docs: ``Gcc also provides a target specific macro
 * __BIGGEST_ALIGNMENT__, which is the largest alignment ever used for any data
 * type on the target machine you are compiling for.'' */
#ifdef __BIGGEST_ALIGNMENT__
# define MAX_ALIGN      __BIGGEST_ALIGNMENT__
#else
/* Crude hack to check that MAX_ALIGN is power-of-two.
 * sizeof(long double) = 12 on i386. */
# define MAX_ALIGN_(n)  (n < 8 ? 8 : \
                         n < 16 ? 16 : n)
# define MAX_ALIGN      MAX_ALIGN_(sizeof (long double))
#endif

static char sos_memory[SOS_MEMORY_SIZE] ALIGNED(MAX_ALIGN);
static size_t sos_memory_freepos;
static size_t pg_size;

HIDDEN void *
sos_alloc (size_t size)
{
  size_t pos;

  size = UNW_ALIGN(size, MAX_ALIGN);

#if defined(__GNUC__) && defined(HAVE_FETCH_AND_ADD)
  /* Assume `sos_memory' is suitably aligned. */
  assert(((uintptr_t) &sos_memory[0] & (MAX_ALIGN-1)) == 0);

  pos = fetch_and_add (&sos_memory_freepos, size);
#else
  static define_lock (sos_lock);
  intrmask_t saved_mask;

  lock_acquire (&sos_lock, saved_mask);
  {
    /* No assumptions about `sos_memory' alignment. */
    if (sos_memory_freepos == 0)
      {
        unsigned align = UNW_ALIGN((uintptr_t) &sos_memory[0], MAX_ALIGN)
                                - (uintptr_t) &sos_memory[0];
        sos_memory_freepos = align;
      }
    pos = sos_memory_freepos;
    sos_memory_freepos += size;
  }
  lock_release (&sos_lock, saved_mask);
#endif

  assert (((uintptr_t) &sos_memory[pos] & (MAX_ALIGN-1)) == 0);
  assert ((pos+size) <= SOS_MEMORY_SIZE);

  return &sos_memory[pos];
}

/* Must be called while holding the mempool lock. */

static void
free_object (struct mempool *pool, void *object)
{
  struct object *obj = object;

  obj->next = pool->free_list;
  pool->free_list = obj;
  ++pool->num_free;
}

static void
add_memory (struct mempool *pool, char *mem, size_t size, size_t obj_size)
{
  char *obj;

  for (obj = mem; obj <= mem + size - obj_size; obj += obj_size)
    free_object (pool, obj);
}

static void
expand (struct mempool *pool)
{
  size_t size;
  char *mem;

  size = pool->chunk_size;
  GET_MEMORY (mem, size);
  if (!mem)
    {
      size = UNW_ALIGN(pool->obj_size, pg_size);
      GET_MEMORY (mem, size);
      if (!mem)
        {
          /* last chance: try to allocate one object from the SOS memory */
          size = pool->obj_size;
          mem = sos_alloc (size);
        }
    }
  add_memory (pool, mem, size, pool->obj_size);
}

HIDDEN void
mempool_init (struct mempool *pool, size_t obj_size, size_t reserve)
{
  if (pg_size == 0)
    pg_size = getpagesize ();

  memset (pool, 0, sizeof (*pool));

  lock_init (&pool->lock);

  /* round object-size up to integer multiple of MAX_ALIGN */
  obj_size = UNW_ALIGN(obj_size, MAX_ALIGN);

  if (!reserve)
    {
      reserve = pg_size / obj_size / 4;
      if (!reserve)
        reserve = 16;
    }

  pool->obj_size = obj_size;
  pool->reserve = reserve;
  pool->chunk_size = UNW_ALIGN(2*reserve*obj_size, pg_size);

  expand (pool);
}

HIDDEN void *
mempool_alloc (struct mempool *pool)
{
  intrmask_t saved_mask;
  struct object *obj;

  lock_acquire (&pool->lock, saved_mask);
  {
    if (pool->num_free <= pool->reserve)
      expand (pool);

    assert (pool->num_free > 0);

    --pool->num_free;
    obj = pool->free_list;
    pool->free_list = obj->next;
  }
  lock_release (&pool->lock, saved_mask);
  return obj;
}

HIDDEN void
mempool_free (struct mempool *pool, void *object)
{
  intrmask_t saved_mask;

  lock_acquire (&pool->lock, saved_mask);
  {
    free_object (pool, object);
  }
  lock_release (&pool->lock, saved_mask);
}
