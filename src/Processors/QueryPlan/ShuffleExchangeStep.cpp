#include <cstddef>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/ShuffleSendStep.h>
#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include "IO/VarInt.h"

namespace DB
{


std::pair<QueryPlanStepPtr, QueryPlanStepPtr> ShuffleExchangeStep::createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const
{
    size_t num_buckets = getResultBucketCount();
    auto sink = std::make_unique<ShuffleSendStep>(input_headers.front(), exchange_id, key_names, num_buckets);

    auto source = std::make_unique<ShuffleReceiveStep>(output_header.value(), exchange_id, source_shards);

    return {std::move(sink), std::move(source)};
}

void ShuffleExchangeStep::serialize(Serialization & ctx) const
{
    writeVarUInt(result_bucket_count, ctx.out);
    writeVarUInt(key_names.size(), ctx.out);
    for (const String & column : key_names)
        writeStringBinary(column, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ShuffleExchangeStep::deserialize(Deserialization & ctx)
{
    size_t result_bucket_count;
    readVarUInt(result_bucket_count, ctx.in);

    size_t key_count;
    readVarUInt(key_count, ctx.in);
    Strings key_names;
    key_names.reserve(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        String column;
        readStringBinary(column, ctx.in);
        key_names.push_back(std::move(column));
    }
    return std::make_unique<ShuffleExchangeStep>(*ctx.output_header, key_names, result_bucket_count);
}

void registerShuffleExchangeStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ShuffleExchange", ShuffleExchangeStep::deserialize);
}

}
