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

/// Priority increases recorded by cache hits and not yet applied to the priority queue
class PendingPriorityIncreases
{
public:
    PendingPriorityIncreases();
    ~PendingPriorityIncreases();

    /// Current number of pending priority increases taken lock-free, e.g. does not account concurrent changes.
    [[nodiscard]] size_t getSizeApprox() const;

    /// Allocates the buffers, of `buffer_size` increases each, unless they are allocated already.
    /// Must be called before other methods of this class.
    void allocateBuffers(size_t buffer_size);

    /// Returns false, without moving from `increase`, if the buffer of the calling thread is full.
    /// Calls `on_half_full` if it is set and this increase filled the buffer to half.
    [[nodiscard]] bool tryAdd(PendingPriorityIncrease & increase, const std::function<void()> & on_half_full);

    /// Moves up to `max_increases` increases to the end of `increases`. Increases of one thread keep their order.
    void pop(PriorityIncreases & increases, size_t max_increases);

    /// Same, for all the increases in the buffer which the calling thread adds to.
    void popBufferOfCurrentThread(PriorityIncreases & increases);

private:
    class Buffers;

    Buffers & getAllocatedBuffers();

    /// Owns what `buffers` points to. Set once, by `allocateBuffers`.
    std::unique_ptr<Buffers> allocated_buffers;
    std::atomic<Buffers *> buffers = nullptr;
};

}
