#pragma once

#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>

namespace DB
{

/// Partitions the data from 1 logical streams into N logical streams.
class ScatterExchangeStep final : public LogicalExchangeStep
{
public:
    /// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to
    /// before hashing, used to align buckets across both sides of a shuffle join.
    ScatterExchangeStep(SharedHeader input_header_, Names key_names_, size_t result_bucket_count_, DataTypes hash_cast_types_ = {})
        : LogicalExchangeStep(input_header_)
        , key_names(std::move(key_names_))
        , hash_cast_types(std::move(hash_cast_types_))
        , result_bucket_count(result_bucket_count_)
    {
        chassert(hash_cast_types.empty() || hash_cast_types.size() == key_names.size());
    }

    String getName() const override { return "ScatterExchange"; }

    void transformPipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings &) override
    {
        /// Doesn't change the pipeline if executed directly
    }

    const Names & getKeys() const
    {
        return key_names;
    }

    const DataTypes & getHashCastTypes() const
    {
        return hash_cast_types;
    }

    size_t getSourceBucketCount() const override
    {
        return 1;
    }

    size_t getResultBucketCount() const override
    {
        return result_bucket_count;
    }

    std::pair<QueryPlanStepPtr, QueryPlanStepPtr> createSinkAndSourcePair(const String & exchange_id, const Strings & source_shards) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    const Names key_names;
    const DataTypes hash_cast_types;
    const size_t result_bucket_count;
};

}
