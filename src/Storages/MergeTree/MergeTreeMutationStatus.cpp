#include <Storages/MergeTree/MergeTreeMutationStatus.h>

#include <Common/Exception.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
}

void checkMutationStatus(std::optional<MergeTreeMutationStatus> & status, const Strings & mutation_ids)
{
    if (!status)
    {
        assert(mutation_ids.size() == 1);
        throw Exception(ErrorCodes::UNFINISHED, "Mutation {} was killed", mutation_ids[0]);
    }
    else if (!status->is_done && !status->latest_fail_reason.empty())
    {
        throw Exception(
            ErrorCodes::UNFINISHED,
            "Exception happened during execution of mutation{} '{}' with part '{}' reason: '{}'. This error maybe retryable or not. "
            "In case of unretryable error, mutation can be killed with KILL MUTATION query",
            mutation_ids.size() > 1 ? "s" : "",
            boost::algorithm::join(mutation_ids, ", "),
            status->latest_failed_part,
            status->latest_fail_reason);
    }
}

}
