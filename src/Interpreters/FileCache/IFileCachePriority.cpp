#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/Metadata.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Common/CurrentMetrics.h>

namespace DB
{

void FileCacheUsageCounters::add(size_t size_delta, size_t elements_delta) noexcept
{
    size.fetch_add(size_delta, std::memory_order_relaxed);
    elements.fetch_add(elements_delta, std::memory_order_relaxed);
}

void FileCacheUsageCounters::sub(size_t size_delta, size_t elements_delta) noexcept
{
    size.fetch_sub(size_delta, std::memory_order_relaxed);
    elements.fetch_sub(elements_delta, std::memory_order_relaxed);
}

FileCacheUsageCountersPtr FileCacheUsageTracker::getOrCreate(const String & user_id)
{
    std::lock_guard lock(mutex);
    auto & weak_usage = usage_by_user[user_id];
    auto usage = weak_usage.lock();
    if (!usage)
    {
        usage = std::make_shared<FileCacheUsageCounters>();
        weak_usage = usage;
    }
    return usage;
}

std::unordered_map<String, FileCacheUsageStat> FileCacheUsageTracker::snapshotAndPrune()
{
    std::lock_guard lock(mutex);
    std::unordered_map<String, FileCacheUsageStat> result;
    result.reserve(usage_by_user.size());
    for (auto it = usage_by_user.begin(); it != usage_by_user.end();)
    {
        const auto & [user_id, weak_usage] = *it;
        auto usage = weak_usage.lock();
        if (!usage)
        {
            it = usage_by_user.erase(it);
            continue;
        }

        const size_t size = usage->size.load(std::memory_order_relaxed);
        const size_t elements = usage->elements.load(std::memory_order_relaxed);
        if (size != 0 || elements != 0)
            result.emplace(user_id, FileCacheUsageStat{.size = size, .elements = elements});
        ++it;
    }
    return result;
}

IFileCachePriority::IFileCachePriority(QueueType queue_type_, size_t max_size_, size_t max_elements_)
    : queue_type(queue_type_), max_size(max_size_), max_elements(max_elements_)
{
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    State initial_state)
    : Entry(key_, offset_, size_, std::move(key_metadata_), initial_state, false)
{
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    State initial_state,
    bool tracks_usage_)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
    , tracks_usage(tracks_usage_)
    , state(initial_state)
{
}

IFileCachePriority::TrackedEntry::TrackedEntry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    FileCacheUsageCountersPtr usage_counters_,
    State initial_state)
    : Entry(key_, offset_, size_, std::move(key_metadata_), initial_state, true)
    , usage_counters(std::move(usage_counters_))
{
    chassert(usage_counters);
}

IFileCachePriority::EntryPtr IFileCachePriority::createEntry(
    const Key & key,
    size_t offset,
    size_t size,
    KeyMetadataPtr key_metadata,
    FileCacheUsageCountersPtr usage_counters,
    Entry::State initial_state)
{
    if (usage_counters)
    {
        return std::make_shared<TrackedEntry>(
            key, offset, size, std::move(key_metadata), std::move(usage_counters), initial_state);
    }
    return std::make_shared<Entry>(key, offset, size, std::move(key_metadata), initial_state);
}

const FileCacheUsageCountersPtr & IFileCachePriority::getUsageCounters(const Entry & entry)
{
    static const FileCacheUsageCountersPtr no_counters;
    if (!entry.tracksUsage())
        return no_counters;
    return static_cast<const TrackedEntry &>(entry).usage_counters;
}

std::string IFileCachePriority::Entry::toString(const std::string & prefix) const
{
    return fmt::format(
        "{}{}:{}:{} (state: {})",
        prefix, key, offset, size.load(),
        magic_enum::enum_name(state.load(std::memory_order_relaxed)));
}

KeyMetadataPtr IFileCachePriority::Entry::getKeyMetadata() const
{
    auto locked = key_metadata.lock();
    if (!locked)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key metadata is expired for entry {}", toString());
    return locked;
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
    if (!usage_tracker)
        return {};
    return usage_tracker->snapshotAndPrune();
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

IFileCachePriority::IPriorityDump::IPriorityDump() = default;
IFileCachePriority::IPriorityDump::~IPriorityDump() = default;

IFileCachePriority::IPriorityDump::IPriorityDump(const std::vector<FileSegmentInfo> & infos_)
    : infos(infos_)
{
}

void IFileCachePriority::IPriorityDump::merge(const IPriorityDump & other)
{
    infos.insert(infos.end(), other.infos.begin(), other.infos.end());
}

}
