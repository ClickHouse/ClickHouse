#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>

namespace DB
{

std::pair<QueryPlanStepPtr, QueryPlanStepPtr> ShuffleExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header.value(), exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

}
