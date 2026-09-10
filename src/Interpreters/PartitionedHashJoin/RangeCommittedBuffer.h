#pragma once

#include <base/types.h>

#include <atomic>
#include <cstddef>

namespace DB
{

/** One allocation whose memory is accounted and zeroed range by range, not at allocation.
  *
  * The partitioned join's shared hash table is one buffer, but its owner ranges are populated one at a
  * time by the workers that fill them, and the scattered chunk each owner consumes is freed in the same
  * step. Charging the whole buffer to the memory tracker up front would make the post-build peak the sum
  * of the table and the full chunk; charging each range when its owner first touches it keeps the two
  * trading off range by range, which is the accounting the spill gate is built on.
  *
  * The memory comes from the standard allocator (jemalloc) through its untracked entry point, so a build
  * that repeats gets its extents back from the arena's retained pages instead of faulting a fresh mapping
  * and unmapping it at teardown. Reused memory is not zero, so `commit` zeroes exactly its range - which
  * is also what faults fresh pages in on the owner's thread rather than one page fault per cell under
  * the mmap lock - after charging `len` bytes against the current memory tracker (throwing on the limit
  * before anything is written). Nothing may read a range before it is committed. Concurrent commits of
  * disjoint ranges are safe.
  */
class RangeCommittedBuffer
{
public:
    RangeCommittedBuffer() = default;
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
    size_t alignment = 0;
    std::atomic<size_t> committed{0};
};

}
