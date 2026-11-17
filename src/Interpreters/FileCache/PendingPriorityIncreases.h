#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <vector>
#include <Common/CacheLine.h>
#include <Interpreters/FileCache/IFileCachePriority.h>

namespace DB
{

/// A cache hit whose move to the tail of an LRU queue is recorded but not applied yet.
struct PendingPriorityIncrease
{
    std::weak_ptr<IFileCachePriority::Entry> entry;
    std::list<IFileCachePriority::EntryPtr>::iterator iterator;
};

/// Priority increases recorded by cache hits and not yet applied to the priority queue: lock-free ring buffers,
/// as many as CPU cores, each thread always adding to the same one. Hits add without a lock, and increases are
/// popped only under the queue write lock, so there is one reader and the lock is taken once per batch.
class PendingPriorityIncreases
{
public:
    ~PendingPriorityIncreases();

    /// Allocates the buffers of `buffer_size` increases each on the first call, so a queue without hits
    /// has none. Must be called before `add`.
    void allocateIfNeeded(size_t buffer_size);

    enum class AddResult
    {
        Added,
        /// Added, and the buffer of the calling thread is at least half full: time to ask for the increases to be applied.
        AddedAtLeastHalfFull,
        /// Not added: the buffer of the calling thread is full.
        Full,
    };

    /// Moves from `increase` only if it is added.
    AddResult add(PendingPriorityIncrease & increase);

    /// Moves up to `max_increases` increases to the end of `increases`. Increases of one thread keep their order.
    /// Pops at most the buffers' capacity, so a call ends while hits keep adding. Requires the queue write lock.
    void pop(std::vector<PendingPriorityIncrease> & increases, size_t max_increases);

    /// Same, for all the increases in the buffer which the calling thread adds to.
    void popBufferOfCurrentThread(std::vector<PendingPriorityIncrease> & increases);

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
