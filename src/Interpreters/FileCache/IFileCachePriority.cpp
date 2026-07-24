#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/Metadata.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void FileCacheUsageCounters::add(size_t size_delta, size_t elements_delta) noexcept
{
    size.fetch_add(size_delta, std::memory_order_relaxed);
    elements.fetch_add(elements_delta, std::memory_order_relaxed);
}

void FileCacheUsageCounters::sub(size_t size_delta, size_t elements_delta) noexcept
{
    const size_t previous_size = size.fetch_sub(size_delta, std::memory_order_relaxed);
    const size_t previous_elements = elements.fetch_sub(elements_delta, std::memory_order_relaxed);
    chassert(previous_size >= size_delta);
    chassert(previous_elements >= elements_delta);
}

FileCacheUsageCounters * FileCacheUsageTracker::getOrSet(const String & user_id)
{
    std::lock_guard lock(mutex);
    auto [it, inserted] = usage_by_user.try_emplace(user_id);
    if (inserted)
        it->second = std::make_unique<FileCacheUsageCounters>();
    return it->second.get();
}

std::unordered_map<String, std::pair<size_t, size_t>> FileCacheUsageTracker::snapshot() const
{
    std::lock_guard lock(mutex);
    std::unordered_map<String, std::pair<size_t, size_t>> result;
    result.reserve(usage_by_user.size());
    for (const auto & [user_id, usage] : usage_by_user)
    {
        result.emplace(
            user_id,
            std::pair{
                usage->size.load(std::memory_order_relaxed),
                usage->elements.load(std::memory_order_relaxed)});
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
    FileCacheUsageCounters * usage_counters_,
    State initial_state)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
    , usage_counters(usage_counters_)
    , state(initial_state)
{
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_,
    State initial_state)
    : Entry(key_, offset_, size_, std::move(key_metadata_), nullptr, initial_state)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
    , usage_counters(other.usage_counters)
{
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

    std::unordered_map<std::string, UsageStat> result;
    for (const auto & [user_id, usage] : usage_tracker->snapshot())
        result.emplace(user_id, UsageStat{.size = usage.first, .elements = usage.second});
    return result;
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
