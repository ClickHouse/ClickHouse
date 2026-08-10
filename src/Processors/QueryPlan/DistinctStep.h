#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/types.h>

namespace DB
{

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
        const SharedHeader & input_header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        /// If is enabled, execute distinct for separate streams, otherwise for merged streams.
        bool pre_distinct_,
        UInt64 set_limit_for_enabling_bloom_filter_ = 0,
        UInt64 bloom_filter_bytes_ = 0,
        Float64 pass_ratio_threshold_for_disabling_bloom_filter_ = 0,
        Float64 max_ratio_of_set_bits_in_bloom_filter_ = 0);

    String getName() const override { return "Distinct"; }
    const Names & getColumnNames() const { return columns; }

    String getSerializationName() const override { return pre_distinct ? "PreDistinct" : "Distinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool isPreliminary() const { return pre_distinct; }

    UInt64 getSetLimitForEnablingBloomFilter() const { return set_limit_for_enabling_bloom_filter; }
    UInt64 getBloomFilterBytes() const { return bloom_filter_bytes; }
    Float64 getBloomFilterPassRatioThreshold() const { return pass_ratio_threshold_for_disabling_bloom_filter; }
    Float64 getBloomFilterMaxRatioSetBits() const { return max_ratio_of_set_bits_in_bloom_filter; }

    UInt64 getLimitHint() const { return limit_hint; }
    void updateLimitHint(UInt64 hint);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx, bool pre_distinct_);
    static QueryPlanStepPtr deserializeNormal(Deserialization & ctx);
    static QueryPlanStepPtr deserializePre(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }

    void applyOrder(SortDescription sort_desc) { distinct_sort_desc = std::move(sort_desc); }
    const SortDescription & getSortDescription() const override { return distinct_sort_desc; }

    /// Each input stream contains a disjoint set of the DISTINCT key values (e.g. because each stream
    /// corresponds to a separate partition and the partition key is a function of the DISTINCT columns).
    /// In that case the final DISTINCT can deduplicate every stream independently and skip merging them
    /// into a single stream.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    void updateOutputHeader() override;

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    SortDescription distinct_sort_desc;
    bool skip_stream_merging = false;

    UInt64 set_limit_for_enabling_bloom_filter;
    UInt64 bloom_filter_bytes;
    Float64 pass_ratio_threshold_for_disabling_bloom_filter;
    Float64 max_ratio_of_set_bits_in_bloom_filter;
};

}
