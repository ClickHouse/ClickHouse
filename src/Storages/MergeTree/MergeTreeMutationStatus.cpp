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
    if (counted_parts.contains(info))
        return;
    for (auto it = counted_parts.begin(); it != counted_parts.end();)
    {
        if (info.contains(it->first))
        {
            bytes -= it->second;
            it = counted_parts.erase(it);
        }
        else if (it->first.contains(info))
        {
            /// A piece of an already-counted range keeps that range's weight.
            return;
        }
        else
            ++it;
    }
    counted_parts.emplace(info, part_bytes);
    bytes += part_bytes;
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
