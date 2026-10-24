#include <Storages/MergeTree/MergeTreeMutationStatus.h>

#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
    extern const int LOGICAL_ERROR;
}

void MutationScopeInitialBytes::account(const MergeTreePartInfo & info, UInt64 part_bytes)
{
    /// The map is ordered by (kind, partition, min_block, ...): entries covered by `info` start at
    /// its range start, and with pairwise-disjoint block ranges a coverer sits just before that.
    MergeTreePartInfo range_start = info;
    range_start.max_block = info.min_block;
    range_start.level = 0;
    range_start.mutation = 0;
    const auto range_begin = counted_parts.lower_bound(range_start);

    for (auto prev = range_begin; prev != counted_parts.begin();)
    {
        --prev;
        if (prev->first.getKind() != info.getKind() || prev->first.getPartitionId() != info.getPartitionId()
            || prev->first.max_block < info.max_block)
            break;
        /// A piece of an already-counted range keeps that range's weight.
        if (prev->first.contains(info))
            return;
    }

    for (auto it = range_begin; it != counted_parts.end()
         && it->first.getKind() == info.getKind() && it->first.getPartitionId() == info.getPartitionId()
         && it->first.min_block <= info.max_block;)
    {
        if (info.contains(it->first))
        {
            bytes -= it->second;
            it = counted_parts.erase(it);
            continue;
        }
        if (it->first.contains(info))
            return;
        ++it;
    }
    counted_parts.emplace(info, part_bytes);
    bytes += part_bytes;
}

void MutationScopeInitialBytes::finalize()
{
    counted_parts.clear();
}

void checkMutationStatus(std::optional<MergeTreeMutationStatus> & status, const std::set<String> & mutation_ids)
{
    if (mutation_ids.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot check mutation status because no mutation ids provided");

    if (!status)
    {
        throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed", *mutation_ids.begin());
    }
    if (!status->is_done && !status->latest_fail_reason.empty())
    {
        throw Exception(
            ErrorCodes::UNFINISHED,
            "Exception happened during execution of mutation{} '{}' with part '{}' reason: '{}'. This error maybe retryable or not. "
            "In case of unretryable error, mutation can be killed with KILL MUTATION query \n\n{}\n",
            mutation_ids.size() > 1 ? "s" : "",
            boost::algorithm::join(mutation_ids, ", "),
            status->latest_failed_part,
            status->latest_fail_reason, StackTrace().toString());
    }
}

}
