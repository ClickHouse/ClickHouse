#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

namespace DB
{

static bool canIncludeToRange(size_t part_size, time_t part_ttl, time_t current_time, size_t usable_memory)
{
    return (0 < part_ttl && part_ttl <= current_time) && usable_memory >= part_size;
}

class ITTLMergeSelector::MergeRangesConstructor
{
    std::optional<PartsRange> buildRange(const CenterPosition & center_position, size_t max_total_size_to_merge)
    {
        const auto & [range, center, _] = center_position;
        if (center->size > max_total_size_to_merge)
            return std::nullopt;

        if (disjoint_set.isCovered(range, center))
            return std::nullopt;

        size_t usable_memory = max_total_size_to_merge - center->size;
        PartsIterator left = merge_selector.findLeftRangeBorder(center_position, usable_memory, disjoint_set);
        PartsIterator right = merge_selector.findRightRangeBorder(center_position, usable_memory, disjoint_set);

        if (range_filter && !range_filter({left, right}))
            return std::nullopt;

        if (!disjoint_set.addRangeIfPossible(range, left, right))
            return std::nullopt;

        return PartsRange(left, right);
    }

public:
    explicit MergeRangesConstructor(const ITTLMergeSelector & merge_selector_, const PartsRanges & parts_ranges, const RangeFilter & range_filter_)
        : merge_selector(merge_selector_)
        , range_filter(range_filter_)
        , disjoint_set(parts_ranges)
        , centers(merge_selector.findCenters(parts_ranges))
    {
    }

    std::optional<PartsRange> buildMergeRange(size_t max_total_size_to_merge)
    {
        constexpr static auto range_compare = [](const CenterPosition & lhs, const CenterPosition & rhs)
        {
            return lhs.ttl > rhs.ttl;
        };

        if (!is_heap_constructed)
        {
            std::make_heap(centers.begin(), centers.end(), range_compare);
            is_heap_constructed = true;
        }

        while (!centers.empty())
        {
            std::pop_heap(centers.begin(), centers.end(), range_compare);
            const auto center = std::move(centers.back());
            centers.pop_back();

            if (auto range = buildRange(center, max_total_size_to_merge))
                return range;
        }

        return std::nullopt;
    }

private:
    const ITTLMergeSelector & merge_selector;
    const RangeFilter & range_filter;

    DisjointPartsRangesSet disjoint_set;
    std::vector<CenterPosition> centers;
    bool is_heap_constructed = false;
};

bool ITTLMergeSelector::needToPostponePartition(const std::string & partition_id) const
{
    if (auto it = merge_due_times.find(partition_id); it != merge_due_times.end())
        return it->second > current_time;

    return false;
}

std::vector<ITTLMergeSelector::CenterPosition> ITTLMergeSelector::findCenters(const PartsRanges & parts_ranges) const
{
    chassert(!parts_ranges.empty());
    std::vector<CenterPosition> centers;

    for (auto range = parts_ranges.begin(); range != parts_ranges.end(); ++range)
    {
        assert(!range->empty());
        const auto & range_partition = range->front().info.getPartitionId();

        if (needToPostponePartition(range_partition))
            continue;

        for (auto part = range->begin(); part != range->end(); ++part)
        {
            if (!canConsiderPart(*part))
                continue;

            time_t ttl = getTTLForPart(*part);
            if (!ttl || ttl > current_time)
                continue;

            centers.emplace_back(range, part, ttl);
        }
    }

    return centers;
}

PartsIterator ITTLMergeSelector::findLeftRangeBorder(const CenterPosition & center_position, size_t & usable_memory, DisjointPartsRangesSet & disjoint_set) const
{
    PartsIterator left = center_position.center;

    while (left != center_position.range->begin())
    {
        auto next_to_check = std::prev(left);
        if (!canConsiderPart(*next_to_check))
            break;

        if (disjoint_set.isCovered(center_position.range, next_to_check))
            break;

        auto ttl = getTTLForPart(*next_to_check);
        if (!canIncludeToRange(next_to_check->size, ttl, current_time, usable_memory))
            break;

        usable_memory -= next_to_check->size;
        left = next_to_check;
    }

    return left;
}

PartsIterator ITTLMergeSelector::findRightRangeBorder(const CenterPosition & center_position, size_t & usable_memory, DisjointPartsRangesSet & disjoint_set) const
{
    PartsIterator right = std::next(center_position.center);

    while (right != center_position.range->end())
    {
        if (!canConsiderPart(*right))
            break;

        if (disjoint_set.isCovered(center_position.range, right))
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

PartsRanges ITTLMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeSizes & max_merge_sizes,
    const RangeFilter & range_filter) const
{
    MergeRangesConstructor constructor(*this, parts_ranges, range_filter);

    PartsRanges result;
    for (size_t max_merge_size : max_merge_sizes)
    {
        if (auto range = constructor.buildMergeRange(max_merge_size))
            result.push_back(std::move(range.value()));
        else
            break;
    }

    return result;
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
