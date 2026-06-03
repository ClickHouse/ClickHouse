#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

namespace DB
{

static bool canIncludeToRange(size_t part_size, size_t part_rows, time_t part_ttl, time_t current_time, size_t usable_memory, size_t usable_rows)
{
    return (0 < part_ttl && part_ttl <= current_time) && usable_memory >= part_size && usable_rows >= part_rows;
}

class ITTLMergeSelector::MergeRangesConstructor
{
    std::optional<PartsRange> buildRange(const CenterPosition & center_position, const MergeConstraint & constraint)
    {
        const auto & [range, center, _] = center_position;
        if (center->size > constraint.max_size_bytes)
            return std::nullopt;

        if (center->rows > constraint.max_size_rows)
            return std::nullopt;

        if (disjoint_set.isCovered(range, center))
            return std::nullopt;

        size_t usable_memory = constraint.max_size_bytes - center->size;
        size_t usable_rows = constraint.max_size_rows - center->rows;
        size_t usable_parts = merge_selector.max_parts_to_merge_at_once ? merge_selector.max_parts_to_merge_at_once - 1 : std::numeric_limits<size_t>::max();
        PartsIterator left = merge_selector.findLeftRangeBorder(center_position, usable_memory, usable_rows, usable_parts, disjoint_set);
        PartsIterator right = merge_selector.findRightRangeBorder(center_position, usable_memory, usable_rows, usable_parts, disjoint_set);

        if (range_filter && !range_filter({left, right}))
            return std::nullopt;

        if (!disjoint_set.addRangeIfPossible(range, left, right))
            return std::nullopt;

        return PartsRange(left, right);
    }

public:
    explicit MergeRangesConstructor(
        const ITTLMergeSelector & merge_selector_,
        const PartsRanges & parts_ranges,
        const RangeFilter & range_filter_)
        : merge_selector(merge_selector_)
        , range_filter(range_filter_)
        , disjoint_set(parts_ranges)
        , centers(merge_selector.findCenters(parts_ranges))
    {
    }

    std::optional<PartsRange> buildMergeRange(const MergeConstraint & constraint)
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

            if (auto range = buildRange(center, constraint))
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
    if (merge_due_times)
        if (auto it = merge_due_times->find(partition_id); it != merge_due_times->end())
            return it->second > current_time;

    return false;
}

std::vector<ITTLMergeSelector::CenterPosition> ITTLMergeSelector::findCenters(const PartsRanges & parts_ranges) const
{
    chassert(!parts_ranges.empty());
    std::vector<CenterPosition> centers;

    for (auto range = parts_ranges.begin(); range != parts_ranges.end(); ++range)
    {
        chassert(!range->empty());
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

PartsIterator ITTLMergeSelector::findLeftRangeBorder(
    const CenterPosition & center_position,
    size_t & usable_memory,
    size_t & usable_rows,
    size_t & usable_parts,
    DisjointPartsRangesSet & disjoint_set) const
{
    PartsIterator left = center_position.center;

    while (left != center_position.range->begin())
    {
        if (!usable_parts)
            break;

        auto next_to_check = std::prev(left);
        if (!canIncludeInRange(*next_to_check))
            break;

        if (disjoint_set.isCovered(center_position.range, next_to_check))
            break;

        auto ttl = getTTLForPart(*next_to_check);
        if (!canIncludeToRange(next_to_check->size, next_to_check->rows, ttl, current_time, usable_memory, usable_rows))
            break;

        usable_memory -= next_to_check->size;
        usable_rows -= next_to_check->rows;
        usable_parts -= 1;
        left = next_to_check;
    }

    return left;
}

PartsIterator ITTLMergeSelector::findRightRangeBorder(
    const CenterPosition & center_position,
    size_t & usable_memory,
    size_t & usable_rows,
    size_t & usable_parts,
    DisjointPartsRangesSet & disjoint_set) const
{
    PartsIterator right = std::next(center_position.center);

    while (right != center_position.range->end())
    {
        if (!usable_parts)
            break;

        if (!canIncludeInRange(*right))
            break;

        if (disjoint_set.isCovered(center_position.range, right))
            break;

        auto ttl = getTTLForPart(*right);
        if (!canIncludeToRange(right->size, right->rows, ttl, current_time, usable_memory, usable_rows))
            break;

        usable_memory -= right->size;
        usable_rows -= right->rows;
        usable_parts -= 1;
        right = std::next(right);
    }

    return right;
}

ITTLMergeSelector::ITTLMergeSelector(
    const PartitionIdToTTLs * merge_due_times_,
    time_t current_time_,
    size_t max_parts_to_merge_at_once_)
    : current_time(current_time_)
    , merge_due_times(merge_due_times_)
    , max_parts_to_merge_at_once(max_parts_to_merge_at_once_)
{
}

PartsRanges ITTLMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeConstraints & merge_constraints,
    const RangeFilter & range_filter) const
{
    MergeRangesConstructor constructor(*this, parts_ranges, range_filter);

    PartsRanges result;
    for (const auto & constraint : merge_constraints)
    {
        if (auto range = constructor.buildMergeRange(constraint))
            result.push_back(std::move(range.value()));
        else
            break;
    }

    return result;
}

TTLPartDropMergeSelector::TTLPartDropMergeSelector(time_t current_time_, size_t max_parts_to_drop_at_once_)
    : ITTLMergeSelector(/*merge_due_times_=*/nullptr, current_time_, max_parts_to_drop_at_once_)
{
}

time_t TTLPartDropMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.general_ttl_info->part_max_ttl;
}

bool TTLPartDropMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (!part.general_ttl_info.has_value())
        return false;

    /// Skip parts whose `part_max_ttl`-contributing TTLs are all marked as
    /// finished. Without this gate `TTLDrop` would re-pick the same part on
    /// every scheduler tick — there is no per-partition cooldown for
    /// `TTLDrop` (`MergeTreeDataMergerMutator::updateTTLMergeTimes` is a
    /// no-op for it). See issue #105647, where `TTL ... GROUP BY`
    /// re-aggregates the same single-row part forever.
    ///
    /// We deliberately consult `has_any_non_finished_rows_affecting_ttls`
    /// here rather than `has_any_non_finished_ttls`: `moves_ttl` and
    /// `recompression_ttl` entries are never marked `finished` and do not
    /// feed `part_max_ttl`, so a table that combines a finished rows /
    /// group-by / column TTL with a move or recompression TTL would
    /// otherwise keep this gate open and reproduce the same infinite loop.
    return part.general_ttl_info->has_any_non_finished_rows_affecting_ttls;
}

bool TTLPartDropMergeSelector::canIncludeInRange(const PartProperties & part) const
{
    /// Center selection already passed `canConsiderPart` for some other part
    /// in this range. A finished neighbor is harmless to fold in: its
    /// `part_max_ttl` was set when it was last touched, persists across
    /// merges, and `canIncludeToRange` (in this file) still gates inclusion
    /// on that value being expired. Allowing it here just lets one TTL merge
    /// sweep adjacent finished + unfinished parts together rather than
    /// leaving the finished one for a later regular merge.
    return part.general_ttl_info.has_value();
}

TTLRowDeleteMergeSelector::TTLRowDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(&merge_due_times_, current_time_)
{
}

time_t TTLRowDeleteMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.general_ttl_info->part_min_ttl;
}

bool TTLRowDeleteMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (part.is_in_volume_where_merges_avoid)
        return false;

    if (!part.general_ttl_info.has_value())
        return false;

    /// Same reasoning as `TTLPartDropMergeSelector::canConsiderPart`:
    /// `getTTLForPart` here returns `part_min_ttl`, which is fed by the same
    /// four TTL kinds (table/columns/rows_where/group_by). Gating on the
    /// broader `has_any_non_finished_ttls` would let an unfinished move or
    /// recompression TTL re-schedule a `TTLDelete` once per
    /// `merge_with_ttl_timeout` window for an otherwise already-finished
    /// part. The cooldown bounds the frequency but not the spuriousness.
    return part.general_ttl_info->has_any_non_finished_rows_affecting_ttls;
}

bool TTLRowDeleteMergeSelector::canIncludeInRange(const PartProperties & part) const
{
    /// See the matching override on `TTLPartDropMergeSelector`. The volume
    /// guard still applies — a finished neighbor on a no-merge volume must
    /// stay out.
    if (part.is_in_volume_where_merges_avoid)
        return false;

    return part.general_ttl_info.has_value();
}

TTLRecompressMergeSelector::TTLRecompressMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(&merge_due_times_, current_time_)
{
}

time_t TTLRecompressMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.recompression_ttl_info->next_recompress_ttl;
}

bool TTLRecompressMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (part.is_in_volume_where_merges_avoid)
        return false;

    if (!part.recompression_ttl_info.has_value())
        return false;

    /// Allow part recompression only if it will change codec. Otherwise there will be no difference in bytes size.
    return part.recompression_ttl_info->will_change_codec;
}

}
