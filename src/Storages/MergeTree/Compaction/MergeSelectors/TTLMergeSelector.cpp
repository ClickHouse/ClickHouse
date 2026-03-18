#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

#include <cassert>
#include <optional>

namespace DB
{

static bool canIncludeToRange(size_t part_size, time_t part_ttl, time_t current_time, size_t usable_memory)
{
    return (0 < part_ttl && part_ttl <= current_time) && usable_memory >= part_size;
}

bool ITTLMergeSelector::needToPostponePartition(const std::string & partition_id) const
{
    if (auto it = merge_due_times.find(partition_id); it != merge_due_times.end())
        return it->second > current_time;

    return false;
}

std::optional<ITTLMergeSelector::CenterPosition> ITTLMergeSelector::findCenter(const PartsRanges & parts_ranges) const
{
    assert(!parts_ranges.empty());
    std::optional<CenterPosition> position = std::nullopt;

    for (auto range = parts_ranges.begin(); range != parts_ranges.end(); ++range)
    {
        assert(!range->empty());
        const auto & range_partition = range->front().info.partition_id;

        if (needToPostponePartition(range_partition))
            continue;

        for (auto part = range->begin(); part != range->end(); ++part)
        {
            if (!canConsiderPart(*part))
                continue;

            time_t ttl = getTTLForPart(*part);
            if (!ttl || ttl > current_time)
                continue;

            if (!position || ttl < getTTLForPart(*position->center))
                position.emplace(range, part);
        }
    }

    return position;
}

ITTLMergeSelector::PartsIterator ITTLMergeSelector::findLeftRangeBorder(PartsIterator left, PartsIterator begin, size_t & usable_memory) const
{
    while (left != begin)
    {
        auto next_to_check = std::prev(left);
        if (!canConsiderPart(*next_to_check))
            break;

        auto ttl = getTTLForPart(*next_to_check);
        if (!canIncludeToRange(next_to_check->size, ttl, current_time, usable_memory))
            break;

        usable_memory -= next_to_check->size;
        left = next_to_check;
    }

    return left;
}

ITTLMergeSelector::PartsIterator ITTLMergeSelector::findRightRangeBorder(PartsIterator right, PartsIterator end, size_t & usable_memory) const
{
    while (right != end)
    {
        if (!canConsiderPart(*right))
            break;

        auto ttl = getTTLForPart(*right);
        if (!canIncludeToRange(right->size, ttl, current_time, usable_memory))
            break;

        usable_memory -= right->size;
        right = std::next(right);
    }

    return right;
}

ITTLMergeSelector::ITTLMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : current_time(current_time_)
    , merge_due_times(merge_due_times_)
{
}

PartsRange ITTLMergeSelector::select(const PartsRanges & parts_ranges, size_t max_total_size_to_merge, RangeFilter range_filter) const
{
    auto position = findCenter(parts_ranges);
    if (!position.has_value())
        return {};

    auto [range, center] = std::move(position.value());
    if (center->size > max_total_size_to_merge)
        return {};

    size_t usable_memory = [max_total_size_to_merge, center]()
    {
        if (max_total_size_to_merge == 0)
            return std::numeric_limits<size_t>::max();

        return max_total_size_to_merge - center->size;
    }();

    PartsIterator left = findLeftRangeBorder(center, range->begin(), usable_memory);
    PartsIterator right = findRightRangeBorder(std::next(center), range->end(), usable_memory);

    if (range_filter && !range_filter({left, right}))
        return {};

    return PartsRange(left, right);
}

TTLPartDeleteMergeSelector::TTLPartDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
{
}

time_t TTLPartDeleteMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.general_ttl_info->part_max_ttl;
}

bool TTLPartDeleteMergeSelector::canConsiderPart(const PartProperties & part) const
{
    return part.general_ttl_info.has_value();
}

TTLRowDeleteMergeSelector::TTLRowDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
{
}

time_t TTLRowDeleteMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.general_ttl_info->part_min_ttl;
}

bool TTLRowDeleteMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (!part.general_ttl_info.has_value())
        return false;

    return part.general_ttl_info->has_any_non_finished_ttls;
}

TTLRecompressMergeSelector::TTLRecompressMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
{
}

time_t TTLRecompressMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.recompression_ttl_info->next_recompress_ttl;
}

bool TTLRecompressMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (!part.recompression_ttl_info.has_value())
        return false;

    /// Allow part recompression only if it will change codec. Otherwise there will be no difference in bytes size.
    return part.recompression_ttl_info->will_change_codec;
}

}
