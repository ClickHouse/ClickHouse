#pragma once

#include "config.h"

#include <cstddef>

namespace DB::HugePageArena
{

/** A dedicated jemalloc arena whose memory is backed by transparent huge pages, used for large
  * hash-table buffers.
  *
  * Aggregation hash tables are probed randomly over hundreds of megabytes, so on 4 KB pages nearly
  * every probe misses the dTLB. Huge pages remove that, but `madvise(MADV_HUGEPAGE)` marks the VMA
  * rather than the allocation, which makes the obvious implementation unsafe in two ways:
  *
  *   - jemalloc frees with `MADV_DONTNEED` and retains the extent, so the marking outlives the
  *     allocation and whatever is placed there next silently inherits huge pages;
  *   - marking each allocation separately splits the VMA around it. Measured on a 4 GB region,
  *     marking 1024 sub-ranges individually added 2047 VMAs.
  *
  * Both are avoided by marking address space rather than allocations. Memory is reserved in large
  * chunks, each marked once with a single `madvise`, and the extent hooks carve allocations out of
  * those chunks. jemalloc only calls the alloc hook when the arena needs new address space - it
  * recycles extents it already owns internally - so the marking cost is one `madvise` per chunk,
  * and the same measurement over a whole chunk added zero VMAs.
  *
  * Nothing marked ever reaches another arena: the dalloc hook retains extents rather than releasing
  * them, so this memory is only ever reused by other hash tables. Purging is still allowed, because
  * `MADV_DONTNEED` does not modify `vm_flags`: it releases the physical pages, keeps the marking,
  * and the range faults back in as huge pages.
  */

/** Matches jemalloc's `oversize_threshold` in `contrib/jemalloc-cmake/CMakeLists.txt`, which is the
  * boundary ClickHouse already uses to separate "rare and transient very large allocation" from
  * everything else. Three reasons it is the right one here too:
  *
  *   - Allocations this large already go to jemalloc's dedicated oversize arena, so routing them to
  *     this one swaps one dedicated arena for another instead of diverting traffic away from the
  *     percpu arenas.
  *   - 2 MB rounding is under 3% of a 64 MiB allocation, and ruinous for a 2 MB one.
  *   - Below it, the first touch of a huge page zeroes 2 MB where 4 KB would have done. `Allocator`
  *     only pre-faults above 16 MiB, so a 2-16 MiB table faulted lazily and paid that cost with no
  *     TLB benefit to show for it. A 2 MiB threshold measurably regressed small hash joins in the
  *     performance suite - `joins_in_memory` by 36-214% on 37-93 ms queries.
  */
constexpr size_t min_allocation_size = 64 * 1024 * 1024;

/// Reads `CH_HASHTABLE_HUGE_PAGES` once. Returns false when jemalloc is not in use, when the
/// environment variable is unset, or when the arena could not be created.
bool isEnabled();

inline bool shouldUse(size_t size)
{
    return size >= min_allocation_size && isEnabled();
}

/// All three assume `shouldUse` was true for the corresponding size. `allocate` returns nullptr on
/// failure so the caller can raise its own exception.
///
/// `zeroed` must be set by allocators that promise cleared memory: jemalloc recycles extents within
/// the arena, so a fresh allocation is not zero-filled just because the address space started that
/// way. For `reallocate` it zeroes only the grown tail.
void * allocate(size_t size, size_t alignment, bool zeroed);
void * reallocate(void * buf, size_t new_size, size_t alignment, bool zeroed);
void deallocate(void * buf);

struct Stats
{
    size_t chunks = 0;
    size_t reserved_bytes = 0;
    size_t carved_bytes = 0;
    /// Chunks where `madvise(MADV_HUGEPAGE)` was rejected; those get 4 KB pages as before.
    size_t failed_madvise = 0;
};

Stats getStats();

}
