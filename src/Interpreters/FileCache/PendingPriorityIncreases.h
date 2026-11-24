#pragma once

#include <algorithm>
#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <boost/container/static_vector.hpp>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/IFileCachePriority.h>

namespace DB
{

struct PendingPriorityIncrease
{
    std::weak_ptr<IFileCachePriority::Entry> entry;
    std::list<IFileCachePriority::EntryPtr>::iterator iterator;
};

/// Increases popped from `PendingPriorityIncreases`. On the stack, so that popping does not allocate.
/// One more than a full buffer, for the hit which found it full.
using PriorityIncreases = boost::container::static_vector<
    PendingPriorityIncrease,
    std::max(FILECACHE_MAX_BATCH_PRIORITY_INCREASE_MAX_BATCH_SIZE, FILECACHE_MAX_BATCH_PRIORITY_INCREASE_BUFFER_SIZE) + 1>;

/// Priority increases recorded by cache hits and not yet applied to the priority queue: lock-free ring buffers,
/// as many as CPU cores, each thread always adding to the same one.
class PendingPriorityIncreases
{
public:
    PendingPriorityIncreases();
    ~PendingPriorityIncreases();

    /// Returns false, without moving from `increase`, if the buffer of the calling thread is full.
    /// Calls `on_half_full` if it is set and this increase filled the buffer to half.
    [[nodiscard]] bool tryAdd(PendingPriorityIncrease & increase, size_t buffer_size, const std::function<void()> & on_half_full);

    /// Moves up to `max_increases` increases to the end of `increases`. Increases of one thread keep their order.
    /// Takes at most one buffer size from each buffer, so a call ends while hits keep adding.
    void pop(PriorityIncreases & increases, size_t max_increases);

    /// Same, for all the increases in the buffer which the calling thread adds to.
    void popBufferOfCurrentThread(PriorityIncreases & increases);

    /// The number of increases, without synchronization with concurrent hits.
    [[nodiscard]] size_t getSizeApprox() const;

private:
    struct Buffers;

    Buffers & getOrAllocateBuffers(size_t buffer_size);

    /// Owns what `buffers` points to. Set once, by the hit which allocates the buffers.
    std::unique_ptr<Buffers> allocated_buffers;
    std::atomic<Buffers *> buffers = nullptr;
};

}
