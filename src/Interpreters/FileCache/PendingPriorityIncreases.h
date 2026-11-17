#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <vector>
#include <Interpreters/FileCache/IFileCachePriority.h>

namespace DB
{

/// A cache hit whose move to the tail of an LRU queue is recorded but not applied yet.
struct PendingPriorityIncrease
{
    std::weak_ptr<IFileCachePriority::Entry> entry;
    std::list<IFileCachePriority::EntryPtr>::iterator iterator;
};

/// Priority increases recorded by cache hits and not yet applied to the priority queue: lock-free rings,
/// as many as CPU cores, each thread always using the same one. Hits add records without a lock, and records
/// are popped only under the queue write lock, so there is one reader and the lock is taken once per batch.
class PendingPriorityIncreases
{
public:
    PendingPriorityIncreases() = default;
    ~PendingPriorityIncreases();

    /// Allocates buffers of `buffer_size` records, one per CPU core, on the first call, so a queue
    /// without hits has none. Must be called before `add`.
    void allocateIfNeeded(size_t buffer_size);

    enum class AddResult
    {
        Added,
        /// Added, and the buffer of the calling thread is at least half full: time to ask for the records to be applied.
        AddedAndHalfFull,
        /// Not added: the buffer of the calling thread is full.
        Full,
    };

    /// Moves from `record` only if it is added.
    AddResult add(PendingPriorityIncrease & record);

    /// Moves up to `max_records` records to the end of `records`. Records of one thread keep their order.
    /// The caller must hold the queue write lock.
    void popRecords(std::vector<PendingPriorityIncrease> & records, size_t max_records);

    /// Same, for all the records present at the call: bounded even while hits keep adding.
    void popAllRecords(std::vector<PendingPriorityIncrease> & records);

    /// Same, for all the records in the buffer of the calling thread.
    void popRecordsOfCurrentThread(std::vector<PendingPriorityIncrease> & records);

    /// The number of records, without synchronization with concurrent hits.
    [[nodiscard]] size_t getSizeApprox() const;

private:
    struct Stripes;

    std::atomic<Stripes *> stripes = nullptr;
    /// Changed only under the queue write lock.
    size_t first_stripe_to_pop = 0;
};

}
