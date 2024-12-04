#include <Parsers/queryToString.h>

#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

namespace DB
{

bool ITTLMergeSelector::canIncludeToRange(Iterator part_it, time_t part_ttl, size_t usable_memory) const
{
    return 0 < part_ttl && part_ttl < current_time
        && canConsiderPart(*part_it)
        && usable_memory >= part_it->size;
}

ITTLMergeSelector::Iterator ITTLMergeSelector::findLeftRangeBorder(Iterator left, Iterator begin, size_t & usable_memory) const
{
    while (left != begin)
    {
        auto next_to_check = std::prev(left);
        auto ttl = getTTLForPart(*next_to_check);

        if (!canIncludeToRange(next_to_check, ttl, usable_memory))
            break;

        left = next_to_check;
        usable_memory -= left->size;
    }

    return left;
}

ITTLMergeSelector::Iterator ITTLMergeSelector::findRightRangeBorder(Iterator right, Iterator end, size_t & usable_memory) const
{
    while (right != end)
    {
        auto ttl = getTTLForPart(*right);

        if (!canIncludeToRange(right, ttl, usable_memory))
            break;

        usable_memory -= right->size;
        right = std::next(right);
    }

    return right;
}

ITTLMergeSelector::ITTLMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : current_time(current_time_)
    , merge_due_times(merge_due_times_)
{
}

PartsRange ITTLMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge) const
{
    Iterator center;
    ssize_t partition_to_merge_index = -1;
    time_t partition_to_merge_min_ttl = 0;

    /// Find most old TTL.
    for (size_t i = 0; i < parts_ranges.size(); ++i)
    {
        const auto & mergeable_parts_in_partition = parts_ranges[i];
        if (mergeable_parts_in_partition.empty())
            continue;

        const auto & partition_id = mergeable_parts_in_partition.front().partition_id;

        if (auto it = merge_due_times.find(partition_id); it != merge_due_times.end() && it->second > current_time)
            continue;

        for (Iterator part_it = mergeable_parts_in_partition.cbegin(); part_it != mergeable_parts_in_partition.cend(); ++part_it)
        {
            time_t ttl = getTTLForPart(*part_it);

            if (ttl && canConsiderPart(*part_it) && (partition_to_merge_index == -1 || ttl < partition_to_merge_min_ttl))
            {
                partition_to_merge_min_ttl = ttl;
                partition_to_merge_index = i;
                center = part_it;
            }
        }
    }

    if (partition_to_merge_index == -1 || partition_to_merge_min_ttl > current_time)
        return {};

    if (center->size > max_total_size_to_merge)
        return {};

    const auto & best_partition = parts_ranges[partition_to_merge_index];
    size_t usable_memory = [max_total_size_to_merge, center]() {
        if (max_total_size_to_merge == 0)
            return std::numeric_limits<size_t>::max();

        return max_total_size_to_merge - center->size;
    }();

    Iterator left = findLeftRangeBorder(center, best_partition.begin(), usable_memory);
    Iterator right = findRightRangeBorder(std::next(center), best_partition.end(), usable_memory);

    return PartsRange(left, right);
}

TTLPartDeleteMergeSelector::TTLPartDeleteMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
{
}

time_t TTLPartDeleteMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.ttl_infos->part_max_ttl;
}

bool TTLPartDeleteMergeSelector::canConsiderPart(const PartProperties &) const
{
    return true;
}

TTLRowDeleteMergeSelector::TTLRowDeleteMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
{
}

time_t TTLRowDeleteMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.ttl_infos->part_min_ttl;
}

bool TTLRowDeleteMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (part.ttl_infos->hasAnyNonFinishedTTLs())
        return part.shall_participate_in_merges;

    /// All TTL satisfied
    return false;
}

TTLRecompressMergeSelector::TTLRecompressMergeSelector(PartitionIdToTTLs & merge_due_times_, time_t current_time_, const TTLDescriptions & recompression_ttls_)
    : ITTLMergeSelector(merge_due_times_, current_time_)
    , recompression_ttls(recompression_ttls_)
{
    chassert(!recompression_ttls.empty());
}

time_t TTLRecompressMergeSelector::getTTLForPart(const PartProperties & part) const
{
    return part.ttl_infos->getMinimalMaxRecompressionTTL();
}

bool TTLRecompressMergeSelector::canConsiderPart(const PartProperties & part) const
{
    if (!part.shall_participate_in_merges)
        return false;

    auto ttl_description = selectTTLDescriptionForTTLInfos(recompression_ttls, part.ttl_infos->recompression_ttl, current_time, /*use_max=*/true);

    if (!ttl_description)
        /// All TTL satisfied
        return false;

    auto ast_to_str = [](ASTPtr query) -> String
    {
        if (!query)
            return "";
        return queryToString(query);
    };

    /// Allow to choose part only if recompression changes codec. Otherwise there will be not difference in memory consumption.
    return ast_to_str(ttl_description->recompression_codec) != ast_to_str(part.compression_codec_desc);
}

}
