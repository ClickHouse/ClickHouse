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

namespace
{
bool checkedSub(std::atomic<size_t> & value, size_t delta) noexcept
{
    size_t current = value.load(std::memory_order_relaxed);
    while (current >= delta)
    {
        if (value.compare_exchange_weak(current, current - delta, std::memory_order_relaxed))
            return true;
    }
    return false;
}
}

void FileCacheUsageCounters::add(size_t size_delta, size_t elements_delta) noexcept
{
    if (!valid.load(std::memory_order_relaxed))
        return;

    size.fetch_add(size_delta, std::memory_order_relaxed);
    elements.fetch_add(elements_delta, std::memory_order_relaxed);
}

void FileCacheUsageCounters::sub(size_t size_delta, size_t elements_delta) noexcept
{
    if (!valid.load(std::memory_order_relaxed))
        return;

    if (!checkedSub(size, size_delta) || !checkedSub(elements, elements_delta))
        valid.store(false, std::memory_order_relaxed);
}

FileCacheUsageCountersPtr FileCacheUsageTracker::getOrCreate(const String & user_id)
{
    std::lock_guard lock(mutex);
    auto [it, inserted] = usage_by_user.try_emplace(user_id);
    if (inserted)
        it->second = std::make_shared<FileCacheUsageCounters>();
    return it->second;
}

std::unordered_map<String, FileCacheUsageStat> FileCacheUsageTracker::snapshot()
{
    std::lock_guard lock(mutex);
    std::unordered_map<String, FileCacheUsageStat> result;
    result.reserve(usage_by_user.size());
    for (auto it = usage_by_user.begin(); it != usage_by_user.end();)
    {
        const auto & [user_id, usage] = *it;
        if (!usage->valid.load(std::memory_order_relaxed))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filesystem cache usage counters became inconsistent for user '{}'", user_id);

        const size_t size = usage->size.load(std::memory_order_relaxed);
        const size_t elements = usage->elements.load(std::memory_order_relaxed);
        if (usage.use_count() == 1)
        {
            if (size != 0 || elements != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Filesystem cache usage counters outlived all entries for user '{}'", user_id);

            it = usage_by_user.erase(it);
            continue;
        }

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
    FileCacheUsageCountersPtr usage_counters_,
    State initial_state)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
    , usage_counters(std::move(usage_counters_))
    , state(initial_state)
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
    return usage_tracker->snapshot();
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
