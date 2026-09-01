#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/GatherReceiveStep.h>

namespace DB
{

std::pair<QueryPlanStepPtr, QueryPlanStepPtr> GatherExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    auto sink = std::make_unique<GatherSendStep>(input_headers.front(), exchange_id, maintain_sort_description);

    auto source = std::make_unique<GatherReceiveStep>(output_header, exchange_id, source_shards.size(), maintain_sort_description);

    return {std::move(sink), std::move(source)};
}

namespace
{
/// Full digest tags for `GatherExchangeStep`, numbered after the base's; never reused.
enum GatherExchangeStepIdentityTag : UInt64
{
    SOURCE_BUCKET_COUNT_TAG = LogicalExchangeStep::FIRST_DERIVED_FULL_DIGEST_TAG,
};
}

void GatherExchangeStep::writeFullDigest(StepDigestWriter & writer) const
{
    writeExchangeBaseFullDigest(writer);

    /// How many buckets are gathered into one; `makeDistributed` prices and wires the exchange on it.
    /// `getResultBucketCount()` needs no tag: it is the constant 1 for this class.
    writer.addVarUInt(SOURCE_BUCKET_COUNT_TAG, source_bucket_count);
}

}
