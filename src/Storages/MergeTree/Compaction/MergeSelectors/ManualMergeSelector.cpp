#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <deque>
#include <mutex>
#include <unordered_map>

namespace DB
{

namespace
{

struct ManualMergeSelectorTableInfo
{
    std::deque<Names> queue;
    std::vector<MergeTreePartInfo> scheduled_part_infos;
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

void ManualMergeSelector::push(const StorageID & id, const Names & parts_to_merge)
{
    auto [info, lock] = getTableInfo(id);

    for (const auto & name : parts_to_merge)
        info->scheduled_part_infos.push_back(MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING));

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

    return info->scheduled_part_infos.empty();
}

void ManualMergeSelector::erase(const StorageID & id)
{
    eraseTableInfo(id);
}

}
