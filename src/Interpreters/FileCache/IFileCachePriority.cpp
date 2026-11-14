#include <Interpreters/FileCache/EvictionCandidates.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/Metadata.h>
#include <base/EnumReflection.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

#include <algorithm>
#include <random>

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

/// Returns the on-disk paths of file segments of a filesystem cache
/// `query_max_elements`: Upper bound on returned rows
/// `query_max_total_size`: upper bound on the sum of size of returned rows; the entry that would cross the bound is not returned.
/// `query_order`: Defines the order on returned rows
std::vector<FileSegmentSummaryInfo>
IFileCachePriority::observeFilesystemCache(UInt64 query_max_elements, UInt64 query_max_total_size, IterationOrder query_order)
{
    auto infos = iterate(query_max_elements, query_max_total_size, query_order, /* start_priority_rank */ 1, QueueEntryType::None).infos;
    if (query_order != IterationOrder::RANDOM)
        return infos;

    /// The sample ignores `query_max_total_size`: applying it during the walk would favour the coldest segments.
    /// Instead, shuffle the sample and keep its prefix, stopping at the first segment which does not fit.
    pcg64 rng(randomSeed());
    std::shuffle(infos.begin(), infos.end(), rng);

    /// `total_size <= query_max_total_size` always holds, so `query_max_total_size - total_size` does not overflow,
    /// while `total_size + info.size` could.
    UInt64 total_size = 0;
    size_t count = 0;
    for (; count < infos.size(); ++count)
    {
        if (infos[count].size > query_max_total_size - total_size)
            break;
        total_size += infos[count].size;
    }
    infos.erase(infos.begin() + count, infos.end());

    std::ranges::sort(infos, {}, &FileSegmentSummaryInfo::priority_rank);
    return infos;
}

FileSegmentSummaryInfos
IFileCachePriority::mergeSamples(FileSegmentSummaryInfos first, FileSegmentSummaryInfos second, UInt64 query_max_elements)
{
    chassert(first.infos.size() == std::min(query_max_elements, first.population));
    chassert(second.infos.size() == std::min(query_max_elements, second.population));

    pcg64 rng(randomSeed());

    /// Draw `query_max_elements` segments one by one without replacement from the union of both populations,
    /// to find how many of them come from each one (the multivariate hypergeometric distribution).
    UInt64 first_left = first.population;
    UInt64 second_left = second.population;
    UInt64 from_first = 0;
    UInt64 from_second = 0;
    while (from_first + from_second < query_max_elements && first_left + second_left > 0)
    {
        if (std::uniform_int_distribution<UInt64>(0, first_left + second_left - 1)(rng) < first_left)
        {
            ++from_first;
            --first_left;
        }
        else
        {
            ++from_second;
            --second_left;
        }
    }

    /// A uniformly random subset of a uniformly random sample is a uniformly random sample (a partial Fisher-Yates shuffle).
    auto take_random = [&](std::vector<FileSegmentSummaryInfo> & infos, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
            std::swap(infos[i], infos[std::uniform_int_distribution<size_t>(i, infos.size() - 1)(rng)]);
        infos.erase(infos.begin() + count, infos.end());
    };
    take_random(first.infos, from_first);
    take_random(second.infos, from_second);

    FileSegmentSummaryInfos result;
    result.infos = std::move(first.infos);
    result.infos.insert(result.infos.end(), std::make_move_iterator(second.infos.begin()), std::make_move_iterator(second.infos.end()));
    result.population = first.population + second.population;
    return result;
}
}
