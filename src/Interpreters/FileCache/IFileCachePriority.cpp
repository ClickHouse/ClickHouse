#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Interpreters/StorageID.h>

namespace ProfileEvents
{
    extern const Event FilesystemCacheBackgroundRemovedInvalidatedEntries;
    extern const Event FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IFileCachePriority::IFileCachePriority(size_t max_size_, size_t max_elements_)
    : max_size(max_size_), max_elements(max_elements_)
{
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    State initial_state)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
    , state(initial_state)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
    , hits(other.hits.load())
{
}

std::string IFileCachePriority::Entry::toString(const std::string & prefix) const
{
    return fmt::format(
        "{}{}:{}:{} (state: {})",
        prefix, key, offset, size.load(),
        magic_enum::enum_name(state.load(std::memory_order_relaxed)));
}

void IFileCachePriority::check(const CacheStateGuard::Lock & lock) const
{
    if ((max_size != 0 && getSize(lock) > max_size) || (max_elements != 0 && getElementsCount(lock) > max_elements))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache limits violated. "
                        "{}", getStateInfoForLog(lock));
    }

    if (getSize(lock) > (1ull << 63) || getElementsCount(lock) > (1ull << 63))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache became inconsistent. There must be a bug");
}

std::unordered_map<std::string, IFileCachePriority::UsageStat> IFileCachePriority::getUsageStatPerClient()
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "getUsageStatPerClient() is not implemented for {} policy",
        magic_enum::enum_name(getType()));
}

void IFileCachePriority::removeEntries(
    const std::vector<InvalidatedEntryInfo> & entries,
    const CachePriorityGuard::WriteLock & lock)
{
    if (entries.empty())
        return;

    for (const auto & [entry, it] : entries)
    {
        /// We store `entry` shared pointer in addition to `it`
        /// (which is an iterator pointing to the same entry)
        /// because `it` could become invalid,
        /// so we use `entry` to check validity of the iterator.
        const auto entry_state = entry->getState();
        chassert(entry_state == Entry::State::Invalidated || entry_state == Entry::State::Removed,
                 fmt::format("Unexpected state: {}", magic_enum::enum_name(entry_state)));
        if (entry_state != Entry::State::Removed)
            it->remove(lock);
    }
}

void IFileCachePriority::onEntryInvalidated()
{
    const size_t threshold = invalidated_threshold.load(std::memory_order_relaxed);
    const size_t prev = invalidated_count.fetch_add(1, std::memory_order_relaxed);
    if (invalidate_notifier && threshold && prev + 1 == threshold)
        invalidate_notifier();
}

void IFileCachePriority::startup(BackgroundSchedulePool & pool, CachePriorityGuard & cache_guard)
{
    cleanup_guard = &cache_guard;
    cleanup_task = pool.createTask(StorageID::createEmpty(), "FileCacheInvalidatedEntriesCleanup", [this] { cleanupTaskFunc(); });
    /// Propagate the wake hook (and threshold) to all sub-queues so any of them can
    /// nudge the single task.
    setInvalidateNotifier(invalidated_threshold.load(std::memory_order_relaxed), [this] { cleanup_task->schedule(); });
    cleanup_task->scheduleAfter(cleanup_interval_ms);
}

void IFileCachePriority::deactivateBackgroundOperations()
{
    if (cleanup_task)
        cleanup_task->deactivate();
}

void IFileCachePriority::cleanupTaskFunc()
{
    Stopwatch watch;

    const size_t removed = removeInvalidatedEntries(cleanup_batch, *cleanup_guard);

    if (removed)
        ProfileEvents::increment(ProfileEvents::FilesystemCacheBackgroundRemovedInvalidatedEntries, removed);

    ProfileEvents::increment(
        ProfileEvents::FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds, watch.elapsedMilliseconds());

    /// A full batch likely means there is more to clean: come back immediately.
    if (removed && removed == cleanup_batch)
        cleanup_task->schedule();
    else
        cleanup_task->scheduleAfter(cleanup_interval_ms ? cleanup_interval_ms : 1);
}

size_t IFileCachePriority::removeInvalidatedEntries(size_t max_batch, CachePriorityGuard & cache_guard)
{
    if (invalidated_count.load(std::memory_order_relaxed) == 0)
        return 0;

    InvalidatedEntriesInfos entries;
    {
        auto lock = cache_guard.readLock();
        collectInvalidatedEntries(max_batch, entries, lock);
    }

    if (entries.empty())
        return 0;

    {
        auto lock = cache_guard.writeLock();
        removeEntries(entries, lock);
    }
    return entries.size();
}

}
