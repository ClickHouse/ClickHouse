#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct ManualMergeSelectorTableInfo
{
    std::deque<Names> queue;
    std::vector<MergeTreePartInfo> scheduled_part_infos;
    /// Every merge scheduled since the last completed SYNC MERGES, kept (unlike `queue`, which is
    /// drained by select) so we can tell whether a not-yet-existing part is still going to be
    /// produced. See isProducible.
    std::vector<Names> merge_defs;
};

MergeTreePartInfo partInfoFromName(const std::string & name)
{
    return MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
}

/// Result part of merging a contiguous range, named the same way as FutureMergedMutatedPart.
std::string mergeResultName(const Names & merge)
{
    std::vector<MergeTreePartInfo> infos;
    infos.reserve(merge.size());
    for (const auto & name : merge)
        infos.push_back(partInfoFromName(name));
    std::sort(infos.begin(), infos.end());

    UInt32 max_level = 0;
    Int64 max_mutation = 0;
    for (const auto & info : infos)
    {
        max_level = std::max(max_level, info.level);
        max_mutation = std::max(max_mutation, info.mutation);
    }

    return MergeTreePartInfo(infos.front().getPartitionId(), infos.front().min_block, infos.back().max_block, max_level + 1, max_mutation)
        .getPartNameV1();
}

/// Whether `name` either is an active part right now or can still be produced by the scheduled
/// merges from parts that are themselves active or producible. Names are compared as strings
/// (the form select/lookupRange use), so a non-canonical spelling never qualifies. A part that
/// was a scheduled-merge result but has since been consumed or dropped is not producible: the
/// merge that made it has left the queue and its inputs are no longer active.
bool isProducible(const std::string & name, const ActiveDataPartSet & active_set,
    const std::vector<Names> & merge_defs, std::unordered_set<std::string> & visited)
{
    if (active_set.getContainingPart(partInfoFromName(name)) == name)
        return true;

    if (!visited.insert(name).second)
        return false;

    for (const auto & def : merge_defs)
    {
        if (mergeResultName(def) != name)
            continue;

        if (std::ranges::all_of(def, [&](const std::string & input)
                { return isProducible(input, active_set, merge_defs, visited); }))
            return true;
    }

    return false;
}

std::mutex registry_mutex;
std::unordered_map<StorageID, ManualMergeSelectorTableInfo, StorageID::DatabaseAndTableNameAndUUIDHash, StorageID::DatabaseAndTableNameAndUUIDEqual> registry;

std::pair<ManualMergeSelectorTableInfo *, std::unique_lock<std::mutex>> getTableInfo(const StorageID & id)
{
    std::unique_lock<std::mutex> lock(registry_mutex);
    return std::make_pair(&registry[id], std::move(lock));
}

void eraseTableInfo(const StorageID & id)
{
    std::unique_lock<std::mutex> lock(registry_mutex);
    registry.erase(id);
}

std::optional<PartsRange> lookupRange(const PartsRanges & parts_ranges, const Names & scheduled_merge)
{
    const auto check_position = [&](const PartsRange & range, size_t start) -> std::optional<PartsRange>
    {
        for (size_t pos = 0; pos < scheduled_merge.size(); ++pos)
            if (scheduled_merge[pos] != range[start + pos].name)
                return std::nullopt;

        return PartsRange(range.begin() + start, range.begin() + start + scheduled_merge.size());
    };

    for (const auto & range : parts_ranges)
        for (size_t start = 0; start + scheduled_merge.size() <= range.size(); ++start)
            if (auto merge = check_position(range, start))
                return merge;

    return std::nullopt;
}

bool checkPreparedRangeAgainstConstraints(const PartsRange & prepared_range, const MergeConstraint & constraint)
{
    size_t total_bytes = 0;
    size_t total_rows = 0;
    for (const auto & part : prepared_range)
    {
        total_bytes += part.size;
        total_rows += part.rows;
    }

    if (total_bytes <= constraint.max_size_bytes && total_rows <= constraint.max_size_rows)
        return true;

    return false;
}

}

ManualMergeSelector::ManualMergeSelector(StorageID storage_id_)
    : storage_id(std::move(storage_id_))
{
}

PartsRanges ManualMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeConstraints & merge_constraints,
    const RangeFilter & range_filter) const
{
    auto [info, lock] = getTableInfo(storage_id);
    if (info->queue.empty())
        return {};

    PartsRanges ranges;
    for (const auto & constraint : merge_constraints)
    {
        if (info->queue.empty())
            break;

        auto range = lookupRange(parts_ranges, info->queue.front());
        if (!range)
            break;

        if (!checkPreparedRangeAgainstConstraints(range.value(), constraint))
            break;

        if (range_filter && !range_filter(range.value()))
            break;

        info->queue.pop_front();
        ranges.push_back(std::move(range.value()));
    }

    return ranges;
}

void ManualMergeSelector::push(const StorageID & id, const Names & parts_to_merge, const ActiveDataPartSet & active_set)
{
    auto [info, lock] = getTableInfo(id);

    /// Reject inputs that can never be merged, so SYNC MERGES does not wait for them until it times
    /// out. An input is fine if it is active now or still producible by the scheduled merges; the
    /// check runs against the merges scheduled so far, not including this one. See isProducible.
    for (const auto & name : parts_to_merge)
    {
        std::unordered_set<std::string> visited;
        if (!isProducible(name, active_set, info->merge_defs, visited))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "SCHEDULE MERGE: part {} does not exist and is not produced by a previously scheduled merge", name);
    }

    for (const auto & name : parts_to_merge)
        info->scheduled_part_infos.push_back(partInfoFromName(name));

    info->merge_defs.push_back(parts_to_merge);
    info->queue.push_back(parts_to_merge);
}

bool ManualMergeSelector::isAllScheduledPartsCovered(const StorageID & id, const ActiveDataPartSet & active_set)
{
    auto [info, lock] = getTableInfo(id);

    std::erase_if(info->scheduled_part_infos, [&](const MergeTreePartInfo & part_info)
    {
        const std::string containing = active_set.getContainingPart(part_info);
        return !containing.empty() && containing != part_info.getPartNameV1();
    });

    if (!info->scheduled_part_infos.empty())
        return false;

    /// Everything scheduled has materialized; forget the merge definitions so they do not grow
    /// unbounded across many schedule/sync cycles.
    info->merge_defs.clear();
    return true;
}

void ManualMergeSelector::erase(const StorageID & id)
{
    eraseTableInfo(id);
}

}
