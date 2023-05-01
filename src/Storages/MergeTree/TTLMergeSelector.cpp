#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Parsers/queryToString.h>

#include <algorithm>
#include <cmath>


namespace DB
{

const String & getPartitionIdForPart(const ITTLMergeSelector::Part & part_info)
{
    const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);
    return part->info.partition_id;
}


IMergeSelector::PartsRange ITTLMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
    using Iterator = IMergeSelector::PartsRange::const_iterator;
    Iterator best_begin;
    ssize_t partition_to_merge_index = -1;
    time_t partition_to_merge_min_ttl = 0;

    /// Find most old TTL.
    for (size_t i = 0; i < parts_ranges.size(); ++i)
    {
        const auto & mergeable_parts_in_partition = parts_ranges[i];
        if (mergeable_parts_in_partition.empty())
            continue;

        const auto & partition_id = getPartitionIdForPart(mergeable_parts_in_partition.front());
        const auto & next_merge_time_for_partition = merge_due_times[partition_id];
        if (next_merge_time_for_partition > current_time)
            continue;

        for (Iterator part_it = mergeable_parts_in_partition.cbegin(); part_it != mergeable_parts_in_partition.cend(); ++part_it)
        {
            time_t ttl = getTTLForPart(*part_it);

            if (ttl && !isTTLAlreadySatisfied(*part_it) && (partition_to_merge_index == -1 || ttl < partition_to_merge_min_ttl))
            {
                partition_to_merge_min_ttl = ttl;
                partition_to_merge_index = i;
                best_begin = part_it;
            }
        }
    }

    if (partition_to_merge_index == -1 || partition_to_merge_min_ttl > current_time)
        return {};

    const auto & best_partition = parts_ranges[partition_to_merge_index];
    Iterator best_end = best_begin + 1;
    size_t total_size = 0;

    /// Find begin of range with most old TTL.
    while (true)
    {
        time_t ttl = getTTLForPart(*best_begin);

        if (!ttl || isTTLAlreadySatisfied(*best_begin) || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge))
        {
            /// This condition can not be satisfied on first iteration.
            ++best_begin;
            break;
        }

        total_size += best_begin->size;
        if (best_begin == best_partition.begin())
            break;

        --best_begin;
    }

    /// Find end of range with most old TTL.
    while (best_end != best_partition.end())
    {
        time_t ttl = getTTLForPart(*best_end);

        if (!ttl || isTTLAlreadySatisfied(*best_end) || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge))
            break;

        total_size += best_end->size;
        ++best_end;
    }

    const auto & best_partition_id = getPartitionIdForPart(best_partition.front());
    merge_due_times[best_partition_id] = current_time + merge_cooldown_time;

    return PartsRange(best_begin, best_end);
}

time_t TTLDeleteMergeSelector::getTTLForPart(const IMergeSelector::Part & part) const
{
    return only_drop_parts ? part.ttl_infos->part_max_ttl : part.ttl_infos->part_min_ttl;
}

bool TTLDeleteMergeSelector::isTTLAlreadySatisfied(const IMergeSelector::Part & part) const
{
    /// N.B. Satisfied TTL means that TTL is NOT expired.
    /// return true -- this part can not be selected
    /// return false -- this part can be selected

    /// Dropping whole part is an exception to `shall_participate_in_merges` logic.
    if (only_drop_parts)
        return false;

    /// All TTL satisfied
    if (!part.ttl_infos->hasAnyNonFinishedTTLs())
        return true;

    return !part.shall_participate_in_merges;
}

time_t TTLRecompressMergeSelector::getTTLForPart(const IMergeSelector::Part & part) const
{
    return part.ttl_infos->getMinimalMaxRecompressionTTL();
}

bool TTLRecompressMergeSelector::isTTLAlreadySatisfied(const IMergeSelector::Part & part) const
{
    /// N.B. Satisfied TTL means that TTL is NOT expired.
    /// return true -- this part can not be selected
    /// return false -- this part can be selected

    if (!part.shall_participate_in_merges)
        return true;

    if (recompression_ttls.empty())
        return false;

    auto ttl_description = selectTTLDescriptionForTTLInfos(recompression_ttls, part.ttl_infos->recompression_ttl, current_time, true);

    if (!ttl_description)
        return true;

    auto ast_to_str = [](ASTPtr query) -> String
    {
        if (!query)
            return "";
        return queryToString(query);
    };

    return ast_to_str(ttl_description->recompression_codec) == ast_to_str(part.compression_codec_desc);
}

}
