#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Scatter is a special case of Shuffle where the number of source buckets is 1.
/// So we can use ShuffleSend and ShuffleReceive steps as sink and source respectively.
std::pair<QueryPlanStepPtr, QueryPlanStepPtr> ScatterExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    if (source_shards.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ScatterExchangeStep should have one source shard, got {}", source_shards.size());

    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header.value(), exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

}
