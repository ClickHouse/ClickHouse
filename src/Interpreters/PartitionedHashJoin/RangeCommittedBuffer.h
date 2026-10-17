#pragma once

#include <base/types.h>

#include <atomic>
#include <cstddef>

namespace DB
{

/** One allocation whose memory is accounted and zeroed range by range, not at allocation.
  *
  * The partitioned join's `HashJoinTable` is one buffer. Its owner ranges are filled one at a time
  * by the workers, and each owner frees its scattered chunk in the same step. If the whole buffer
  * were charged to the memory tracker up front, the post-build peak would be the table plus the
  * whole chunk. Charging each range when its owner first touches it lets the table's charge rise as
  * the chunk's falls. That is the accounting the spill decision (`max_bytes_before_external_join`)
  * is built on.
  *
  * The memory comes from jemalloc through its untracked entry point. A repeated build gets its
  * extents back from the arena's retained pages instead of faulting a fresh mapping and unmapping it
  * at teardown. Reused memory is not zero. `commit` first charges `len` bytes to the current memory
  * tracker, which throws on the limit before anything is written, then zeroes exactly its range. The
  * zeroing is also what faults fresh pages in on the owner's thread, instead of one page fault per
  * cell under the mmap lock. Nothing may read a range before it is committed. Concurrent commits of
  * disjoint ranges are safe.
  */
class RangeCommittedBuffer
{
public:
    explicit RangeCommittedBuffer(size_t bytes_);
    ~RangeCommittedBuffer();

    RangeCommittedBuffer(const RangeCommittedBuffer &) = delete;
    RangeCommittedBuffer & operator=(const RangeCommittedBuffer &) = delete;
    RangeCommittedBuffer(RangeCommittedBuffer && other) noexcept;
    RangeCommittedBuffer & operator=(RangeCommittedBuffer && other) noexcept;

    char * data() const { return ptr; }
    size_t size() const { return bytes; }
    size_t committedBytes() const { return committed.load(std::memory_order_relaxed); }

    /// Accounts and zeroes `[offset, offset + len)`. Each byte must be committed exactly once.
    void commit(size_t offset, size_t len);

private:
    void reset();

    char * ptr = nullptr;
    size_t bytes = 0;
    std::atomic<size_t> committed{0};
};

}
