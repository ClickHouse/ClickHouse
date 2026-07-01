#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <unordered_map>

namespace DB
{

namespace
{

/// A single SYSTEM SCHEDULE MERGE request. Both things a scheduled merge is tracked by live in one
/// object so they are always retired together: `parts` (the source part names, in order) drives
/// select()/lookupRange(), and `part_infos` (the same parts, parsed) drives the SYNC MERGES coverage
/// check. `assigned` is set once select() has handed the merge to the background executor; the object
/// then stays so SYNC MERGES can keep waiting for the merge's part_log row, and is dropped only by
/// clearScheduledParts() once that command has fully synced it. Keeping one object avoids the
/// divergence where a merge satisfied by a covering part (so select() never matched its sources) is
/// cleared from the coverage list while its stale, impossible-to-match entry survives in a separate
/// queue and starves every later SCHEDULE MERGE on the same table.
struct ScheduledMerge
{
    Names parts;
    std::vector<MergeTreePartInfo> part_infos;
    bool assigned = false;
};

struct ManualMergeSelectorTableInfo
{
    std::deque<ScheduledMerge> scheduled_merges;
};

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

    PartsRanges ranges;
    auto merge_it = info->scheduled_merges.begin();
    for (const auto & constraint : merge_constraints)
    {
        /// Skip merges already handed to the executor. They stay in the deque so SYNC MERGES can
        /// still wait for their part_log row and are removed only by clearScheduledParts(), but they
        /// must not be dispatched again. Pending merges are matched strictly in FIFO order: the first
        /// one whose source parts are not yet present locally stops this cycle, exactly as before.
        while (merge_it != info->scheduled_merges.end() && merge_it->assigned)
            ++merge_it;
        if (merge_it == info->scheduled_merges.end())
            break;

        auto range = lookupRange(parts_ranges, merge_it->parts);
        if (!range)
            break;

        if (!checkPreparedRangeAgainstConstraints(range.value(), constraint))
            break;

        if (range_filter && !range_filter(range.value()))
            break;

        merge_it->assigned = true;
        ranges.push_back(std::move(range.value()));
        ++merge_it;
    }

    return ranges;
}

void ManualMergeSelector::push(const StorageID & id, const Names & parts_to_merge)
{
    auto [info, lock] = getTableInfo(id);

    ScheduledMerge merge;
    merge.parts = parts_to_merge;
    merge.part_infos.reserve(parts_to_merge.size());
    for (const auto & name : parts_to_merge)
        merge.part_infos.push_back(MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING));

    info->scheduled_merges.push_back(std::move(merge));
}

std::vector<MergeTreePartInfo> ManualMergeSelector::getScheduledPartInfos(const StorageID & id)
{
    auto [info, lock] = getTableInfo(id);

    std::vector<MergeTreePartInfo> part_infos;
    for (const auto & merge : info->scheduled_merges)
        part_infos.insert(part_infos.end(), merge.part_infos.begin(), merge.part_infos.end());

    return part_infos;
}

bool ManualMergeSelector::isAllScheduledPartsCovered(const std::vector<MergeTreePartInfo> & scheduled_part_infos, const ActiveDataPartSet & active_set)
{
    /// Pure predicate over a caller-supplied snapshot: a scheduled source part is covered when a
    /// strictly larger active part contains it. Reads no registry state, so SYNC MERGES sees exactly
    /// the set it captured at entry and does not start waiting on parts a concurrent SCHEDULE MERGE
    /// added meanwhile. Does not drain anything -- SYNC MERGES still waits for part_log after this
    /// returns true and may time out or be cancelled, and a retry must still see the scheduled parts.
    /// The set is dropped only by clearScheduledParts() once the command fully succeeds.
    for (const auto & part_info : scheduled_part_infos)
    {
        const std::string containing = active_set.getContainingPart(part_info);
        if (containing.empty() || containing == part_info.getPartNameV1())
            return false;
    }

    return true;
}

void ManualMergeSelector::clearScheduledParts(const StorageID & id, const NameSet & part_names)
{
    auto [info, lock] = getTableInfo(id);

    /// Drop every scheduled merge whose source parts are all in `part_names` (the snapshot SYNC MERGES
    /// captured and has now fully synced). Removing the whole object retires both carriers at once, so
    /// no stale entry survives in select()'s view to starve later schedules -- even when the merge was
    /// satisfied by a covering part and select() never matched its sources. A merge added by a
    /// concurrent SCHEDULE MERGE after the snapshot has a source part outside `part_names` and is left
    /// intact.
    std::erase_if(info->scheduled_merges, [&](const ScheduledMerge & merge)
    {
        return std::all_of(merge.parts.begin(), merge.parts.end(),
            [&](const String & name) { return part_names.contains(name); });
    });
}

void ManualMergeSelector::erase(const StorageID & id)
{
    eraseTableInfo(id);
}

}
