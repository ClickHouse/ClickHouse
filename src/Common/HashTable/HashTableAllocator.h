#pragma once

#include <Common/Allocator.h>


/**
  * We are going to use the entire memory we allocated when resizing a hash
  * table, so it makes sense to pre-fault the pages so that page faults don't
  * interrupt the resize loop. Set the allocator parameter accordingly.
  *
  * Large tables are additionally served from a dedicated jemalloc arena backed by transparent huge
  * pages, when `CH_HASHTABLE_HUGE_PAGES` is set. A table probed randomly over hundreds of megabytes
  * misses the dTLB on nearly every probe at 4 KB pages; see `HugePageArena` for why this needs its
  * own arena rather than an `madvise` on the buffer.
  */
using HashTableAllocator = Allocator<true /* clear_memory */, true /* populate */, true /* huge_pages */>;

template <size_t initial_bytes = 64>
using HashTableAllocatorWithStackMemory = AllocatorWithStackMemory<HashTableAllocator, initial_bytes>;
