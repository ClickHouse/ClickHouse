#pragma once

#include <Common/Allocator.h>


/**
  * We are going to use the entire memory we allocated when resizing a hash
  * table, so it makes sense to pre-fault the pages so that page faults don't
  * interrupt the resize loop. Set the allocator parameter accordingly.
  */
using HashTableAllocator = Allocator<true /* clear_memory */, true /* mmap_populate */>;

template <size_t N = 64>
using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator, N>;
