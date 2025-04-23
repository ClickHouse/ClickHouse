#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/BroadcastSendStep.h>
#include <Processors/QueryPlan/BroadcastReceiveStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::pair<QueryPlanStepPtr, QueryPlanStepPtr> BroadcastExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    if (source_shards.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BroadcastExchangeStep should have one source shard, got {}", source_shards.size());

    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<BroadcastSendStep>(input_headers.front(), exchange_id, num_buckets);

    auto source = std::make_unique<BroadcastReceiveStep>(output_header.value(), exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

}
