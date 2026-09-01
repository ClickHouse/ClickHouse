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
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets, hash_cast_types);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header, exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

namespace
{
/// Full digest tags for `ScatterExchangeStep`, numbered after the base's; never reused.
enum ScatterExchangeStepIdentityTag : UInt64
{
    KEY_NAMES_TAG = LogicalExchangeStep::FIRST_DERIVED_FULL_DIGEST_TAG,
    HASH_CAST_TYPES_TAG,
    RESULT_BUCKET_COUNT_TAG,
};
}

void ScatterExchangeStep::writeFullDigest(StepDigestWriter & writer) const
{
    writeExchangeBaseFullDigest(writer);

    /// Which columns the single input is partitioned by, and what each key is cast to before hashing -
    /// both decide which bucket a row lands in (`ShuffleSendStep` passes them to `scatterByPartition`).
    /// An empty name stands for an absent cast, exactly as on `ShuffleSendStep`'s wire. An empty key
    /// list is a round-robin scatter, which `DistributionEnforcer` builds for a bare node-count
    /// requirement.
    writer.addStrings(KEY_NAMES_TAG, key_names);

    Names hash_cast_type_names;
    hash_cast_type_names.reserve(hash_cast_types.size());
    for (const auto & type : hash_cast_types)
        hash_cast_type_names.push_back(type ? type->getName() : "");
    writer.addStrings(HASH_CAST_TYPES_TAG, hash_cast_type_names);

    /// The modulo of the scatter hash, and what `makeDistributed` prices and wires.
    /// `getSourceBucketCount()` needs no tag: it is the constant 1 for this class.
    writer.addVarUInt(RESULT_BUCKET_COUNT_TAG, result_bucket_count);
}

}
