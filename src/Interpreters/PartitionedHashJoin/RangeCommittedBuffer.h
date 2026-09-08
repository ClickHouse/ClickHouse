#pragma once

#include <base/types.h>

#include <atomic>
#include <cstddef>

namespace DB
{

/** One anonymous mapping whose memory is accounted and faulted in range by range, not at allocation.
  *
  * The partitioned join's shared hash table is one buffer, but its owner ranges are populated one at a
  * time by the workers that fill them, and the scattered chunk each owner consumes is freed in the same
  * step. Charging the whole buffer to the memory tracker up front would make the post-build peak the sum
  * of the table and the full chunk; charging each range when its owner first touches it keeps the two
  * trading off range by range, which is the accounting the spill gate is built on.
  *
  * The mapping is reserved with `MAP_NORESERVE` and never touched by this class except through `commit`,
  * which accounts `len` bytes against the current memory tracker (throwing on the limit before anything is
  * written) and pre-faults the pages so an owner's random-order inserts do not take one page fault per
  * cell under the mmap lock. Fresh anonymous pages read as zero, which is the empty-cell state of every
  * cell type the table holds. Concurrent commits of disjoint ranges are safe: the tracker is thread-safe
  * and the pre-fault is idempotent on shared page edges.
  */
class RangeCommittedBuffer
{
public:
    RangeCommittedBuffer() = default;
    explicit RangeCommittedBuffer(size_t bytes_);
    ~RangeCommittedBuffer();

    RangeCommittedBuffer(const RangeCommittedBuffer &) = delete;
    RangeCommittedBuffer & operator=(const RangeCommittedBuffer &) = delete;

    char * data() const { return ptr; }
    size_t size() const { return bytes; }
    size_t committedBytes() const { return committed.load(std::memory_order_relaxed); }

    /// Accounts and pre-faults `[offset, offset + len)`. Each byte must be committed exactly once.
    void commit(size_t offset, size_t len);

private:
    char * ptr = nullptr;
    size_t bytes = 0;
    std::atomic<size_t> committed{0};
};

}
