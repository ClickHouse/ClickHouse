#include <Storages/MergeTree/MergeTreeMutationStatus.h>

#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <boost/algorithm/string/join.hpp>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
    extern const int LOGICAL_ERROR;
}

void accumulatePartBlockBytes(PartBlockBytes & parts)
{
    std::sort(parts.begin(), parts.end());

    UInt64 running_total = 0;
    for (auto & [block, bytes] : parts)
    {
        running_total += bytes;
        bytes = running_total;
    }
}

UInt64 getBytesBeforeBlock(const PartBlockBytes & parts, Int64 block_number)
{
    auto it = std::partition_point(
        parts.begin(), parts.end(), [&](const auto & part) { return part.first < block_number; });
    return it == parts.begin() ? 0 : std::prev(it)->second;
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
