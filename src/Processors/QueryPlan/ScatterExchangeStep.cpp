#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>

namespace DB
{

/// Scatter is a special case of Shuffle where the number of source buckets is 1, so it reuses the
/// ShuffleSend and ShuffleReceive steps. Those steps do not require a single source bucket
/// (ShuffleSend runs per source bucket, ShuffleReceive reads every source bucket for its destination
/// bucket), so a multi-bucket source is handled correctly: the pair just repartitions all source
/// buckets into the requested result buckets, i.e. behaves as a plain shuffle.
std::pair<QueryPlanStepPtr, QueryPlanStepPtr> ScatterExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets, hash_cast_types);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header, exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

}
