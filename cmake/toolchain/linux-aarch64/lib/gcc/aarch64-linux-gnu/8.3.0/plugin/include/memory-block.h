/* Shared pool of memory blocks for pool allocators.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */


#ifndef MEMORY_BLOCK_H
#define MEMORY_BLOCK_H

/* Shared pool which allows other memory pools to reuse each others' allocated
   memory blocks instead of calling free/malloc again.  */
class memory_block_pool
{
public:
  /* Blocks have fixed size.  This is necessary for sharing.  */
  static const size_t block_size = 64 * 1024;

  memory_block_pool ();

  static inline void *allocate () ATTRIBUTE_MALLOC;
  static inline void release (void *);
  void clear_free_list ();

private:
  /* memory_block_pool singleton instance, defined in memory-block.cc.  */
  static memory_block_pool instance;

  struct block_list
  {
    block_list *m_next;
  };

  /* Free list.  */
  block_list *m_blocks;
};

/* Allocate a single block.  Reuse a previously returned block, if possible.  */
inline void *
memory_block_pool::allocate ()
{
  if (instance.m_blocks == NULL)
    return XNEWVEC (char, block_size);

  void *result = instance.m_blocks;
  instance.m_blocks = instance.m_blocks->m_next;
  VALGRIND_DISCARD (VALGRIND_MAKE_MEM_UNDEFINED (result, block_size));
  return result;
}

/* Return UNCAST_BLOCK to the pool.  */
inline void
memory_block_pool::release (void *uncast_block)
{
  block_list *block = new (uncast_block) block_list;
  block->m_next = instance.m_blocks;
  instance.m_blocks = block;
}

extern void *mempool_obstack_chunk_alloc (size_t) ATTRIBUTE_MALLOC;
extern void mempool_obstack_chunk_free (void *);

#endif /* MEMORY_BLOCK_H */
