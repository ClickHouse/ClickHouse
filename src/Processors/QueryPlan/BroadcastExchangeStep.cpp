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

    auto source = std::make_unique<BroadcastReceiveStep>(output_header, exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

namespace
{
/// Full digest tags for `BroadcastExchangeStep`, numbered after the base's; never reused.
enum BroadcastExchangeStepIdentityTag : UInt64
{
    RESULT_BUCKET_COUNT_TAG = LogicalExchangeStep::FIRST_DERIVED_FULL_DIGEST_TAG,
};
}

void BroadcastExchangeStep::writeFullDigest(StepDigestWriter & writer) const
{
    writeExchangeBaseFullDigest(writer);

    /// How many copies of the input the exchange produces; `makeDistributed` prices and wires it on
    /// this. `getSourceBucketCount()` needs no tag: it is the constant 1 for this class.
    writer.addVarUInt(RESULT_BUCKET_COUNT_TAG, result_bucket_count);
}

}
