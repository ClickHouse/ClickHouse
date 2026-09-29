#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>

namespace DB
{

std::pair<QueryPlanStepPtr, QueryPlanStepPtr> ShuffleExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets, hash_cast_types);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header, exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

namespace
{
/// Full digest tags for `ShuffleExchangeStep`, numbered after the base's; never reused.
enum ShuffleExchangeStepIdentityTag : UInt64
{
    KEY_NAMES_TAG = LogicalExchangeStep::FIRST_DERIVED_FULL_DIGEST_TAG,
    HASH_CAST_TYPES_TAG,
    SOURCE_BUCKET_COUNT_TAG,
    RESULT_BUCKET_COUNT_TAG,
};
}

void ShuffleExchangeStep::writeFullDigest(StepDigestWriter & writer) const
{
    writeExchangeBaseFullDigest(writer);

    /// Which columns the rows are repartitioned by, and what each key is cast to before hashing -
    /// both decide which bucket a row lands in (`ShuffleSendStep` passes them to `scatterByPartition`).
    /// An empty name stands for an absent cast, exactly as on `ShuffleSendStep`'s wire.
    writer.addStrings(KEY_NAMES_TAG, key_names);

    Names hash_cast_type_names;
    hash_cast_type_names.reserve(hash_cast_types.size());
    for (const auto & type : hash_cast_types)
        hash_cast_type_names.push_back(type ? type->getName() : "");
    writer.addStrings(HASH_CAST_TYPES_TAG, hash_cast_type_names);

    /// The bucket counts on both sides of the exchange: the modulo of the shuffle hash, and what
    /// `makeDistributed` prices and wires.
    writer.addVarUInt(SOURCE_BUCKET_COUNT_TAG, source_bucket_count);
    writer.addVarUInt(RESULT_BUCKET_COUNT_TAG, result_bucket_count);
}

}
