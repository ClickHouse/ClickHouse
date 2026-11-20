#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/Metadata.h>
#include <base/EnumReflection.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(key_metadata_ && key_metadata_->useRealDiskSize() ? key_metadata_->alignFileSize(size_) : size_)
    , state(initial_state)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
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
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "getUsageStatPerClient() is not implemented for {} policy",
        magic_enum::enum_name(getType()));
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

namespace
{

/// The summary of the file segment of `entry`, or `nullopt` if the key is being removed, or the segment is gone or empty.
/// Despite its name, `KeyMetadata::tryLock` waits for the key lock and fails only if the key is no longer `ACTIVE`,
/// so a key held by another query makes the walk wait while it holds the priority read lock.
/// The key lock is held only to find the file segment, to make sure it is still in the cache,
/// and, for a `Moving` or `Evicting` entry, to check that it is still the current queue entry of the segment.
/// Everything else comes from the queue entry, the key metadata, or atomics of the file segment.
std::optional<FileSegmentSummaryInfo>
tryGetSummaryInfo(const IFileCachePriority::Entry & entry, const IFileCachePriority::ObservedQueue & observed_queue)
{
    auto key_metadata = entry.key_metadata.lock();
    if (!key_metadata)
        return std::nullopt;

    FileSegmentPtr file_segment;
    {
        auto locked_key = key_metadata->tryLock();
        if (!locked_key)
            return std::nullopt;

        auto file_segment_metadata = locked_key->tryGetByOffset(entry.offset);
        if (!file_segment_metadata)
            return std::nullopt;
        file_segment = file_segment_metadata->file_segment;

        /// The segment was evicted and downloaded again, or moved by SLRU since the entry was copied, or the walk copied
        /// both entries of an SLRU move: the segment is counted only through its current entry.
        /// Lock order: key > file segment (`getQueueIterator`) > `SLRUIterator::entry_mutex` (`getEntry`).
        auto queue_iterator = file_segment->getQueueIterator();
        if (!queue_iterator || queue_iterator->getEntry().get() != &entry)
            return std::nullopt;
    }

    /// A `PreActive` entry has size 0 until its SLRU move is committed: the size is still on the old entry.
    /// The queue entry is always charged with the disk-accounted size of its segment.
    const size_t size
        = entry.getState() == IFileCachePriority::Entry::State::PreActive ? file_segment->getDiskAccountedSize() : entry.size.load();
    if (size == 0)
        return std::nullopt;

    return FileSegmentSummaryInfo{
        .path = key_metadata->getFileSegmentPath(*file_segment),
        .key = entry.key.toString(),
        .offset = entry.offset,
        .size = size,
        .state = FileSegment::stateToString(file_segment->state()),
        .split_queue = observed_queue.split_queue.transform([](std::string_view name) { return String(name); }),
        .slru_queue = observed_queue.slru_queue.transform([](std::string_view name) { return String(name); }),
        .user_id = key_metadata->origin->user_id,
    };
}

}

FileSegmentSummaryInfoList
IFileCachePriority::observeFilesystemCache(
    UInt64 query_max_elements, UInt64 query_max_total_size, IterationOrder query_order, const std::atomic<bool> * cancellation_flag)
{
    if (query_max_elements == 0)
        return {};

    /// Copy the entries under the priority lock, which each queue takes in `iterate`.
    FileSegmentSummaryInfos walk;
    walk.cancellation_flag = cancellation_flag;
    iterate(query_max_elements, query_max_total_size, query_order, ObservedQueue{}, walk);
    if (walk.isCancelled())
        return {};

    /// Read the file segments of the copied entries without the priority lock, so that waiting for a key lock does not block the queue.
    /// The entries whose segments are gone by now are skipped, so the result can have fewer than `query_max_elements` rows.
    FileSegmentSummaryInfoList result;
    result.reserve(walk.entries.size());
    /// Deduplicate SLRU entries that could be moved
    std::unordered_set<FileCacheKeyAndOffset, FileCacheKeyAndOffsetHash> returned_segments;
    returned_segments.reserve(walk.entries.size());
    UInt64 total_size = 0;
    size_t read_entries = 0;
    for (const auto & observed : walk.entries)
    {
        if (++read_entries % observe_check_cancellation_every == 0 && walk.isCancelled())
            return {};

        const auto & entry = *observed.entry;
        if (returned_segments.contains({entry.key, entry.offset}))
            continue;

        auto info = tryGetSummaryInfo(entry, observed.observed_queue);
        if (!info)
            continue;

        /// coldest/hottest: stop before the segment which would push over the limit
        if (query_order != IterationOrder::RANDOM && info->size > query_max_total_size - total_size)
            break;

        returned_segments.emplace(entry.key, entry.offset);
        info->priority_rank = result.size() + 1;
        total_size += info->size;
        result.push_back(std::move(*info));
    }

    if (query_order != IterationOrder::RANDOM)
        return result;

    /// We ignore query_max_total_size in the sampling process because we would favor elements at the start of the default iteration order (hereby coldest)
    /// That's why we shuffle so we don't always remove same segments
    std::shuffle(result.begin(), result.end(), thread_local_rng);

    /// Keep dropping segments from the end so we respect in the end query_max_total_size for RANDOM order
    while (total_size > query_max_total_size)
    {
        total_size -= result.back().size;
        result.pop_back();
    }
    return result;
}
}
