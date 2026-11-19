#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <boost/container/small_vector.hpp>
#include <Common/CacheLine.h>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/IFileCachePriority.h>

namespace DB
{

/// A cache hit whose move to the tail of an LRU queue is recorded but not applied yet.
struct PendingPriorityIncrease
{
    std::weak_ptr<IFileCachePriority::Entry> entry;
    std::list<IFileCachePriority::EntryPtr>::iterator iterator;
};

/// A batch of the default size stays on the stack, so popping one under the queue write lock does not allocate.
using PriorityIncreases
    = boost::container::small_vector<PendingPriorityIncrease, FILECACHE_DEFAULT_BATCH_PRIORITY_INCREASE_MAX_BATCH_SIZE>;

/// Priority increases recorded by cache hits and not yet applied to the priority queue: lock-free ring buffers,
/// as many as CPU cores, each thread always adding to the same one. Hits add without a lock, and increases are
/// popped only under the queue write lock, so there is one reader and the lock is taken once per batch.
class PendingPriorityIncreases
{
public:
    ~PendingPriorityIncreases();

    /// Allocates the buffers of `buffer_size` increases each on the first call, so a queue without hits
    /// has none. Must be called before `tryAdd`.
    void allocateIfNeeded(size_t buffer_size);

    /// Returns false, without moving from `increase`, if the buffer of the calling thread is full.
    /// Calls `on_half_full` if it is set and this increase filled the buffer to half.
    [[nodiscard]] bool tryAdd(PendingPriorityIncrease & increase, const std::function<void()> & on_half_full);

    /// Moves up to `max_increases` increases, or all of them if not set, to the end of `increases`. Increases of one
    /// thread keep their order. Pops at most the buffers' capacity, so a call ends while hits keep adding.
    /// Requires the queue write lock.
    void pop(PriorityIncreases & increases, std::optional<size_t> max_increases);

    /// Same, for all the increases in the buffer which the calling thread adds to.
    void popBufferOfCurrentThread(PriorityIncreases & increases);

    /// The number of increases, without synchronization with concurrent hits.
    [[nodiscard]] size_t getSizeApprox() const;

private:
    struct Buffers;

    /// Read by every hit.
    std::atomic<Buffers *> buffers = nullptr;
    /// Changed only under the queue write lock, on every pop.
    alignas(CH_CACHE_LINE_SIZE) size_t first_buffer_to_pop = 0;
};

}
