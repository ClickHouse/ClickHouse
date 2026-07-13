#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <optional>

namespace DB
{

/// Implements a step that doesn't modify the data but builds a bloom filter from the values of the specified column.
/// This bloom filter is put into a per-query map and can be used with `filterContains` function.
/// This is used for filtering left side af a JOIN based on key values collected from the right side.
class BuildRuntimeFilterStep : public ITransformingStep
{
public:
    BuildRuntimeFilterStep(
        const SharedHeader & input_header_,
        String filter_column_name_,
        const DataTypePtr & filter_column_type_,
        String filter_name_,
        String filter_key_,
        UInt64 exact_values_limit_,
        UInt64 bloom_filter_bytes_,
        UInt64 bloom_filter_hash_functions_,
        Float64 pass_ratio_threshold_for_disabling,
        UInt64 blocks_to_skip_before_reenabling,
        Float64 max_ratio_of_set_bits_in_bloom_filter,
        bool allow_to_use_not_exact_filter_,
        std::optional<UInt64> distinct_keys_hint_ = std::nullopt);

    BuildRuntimeFilterStep(const BuildRuntimeFilterStep & other) = default;

    String getName() const override { return "BuildRuntimeFilter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterColumnName() const { return filter_column_name; }
    const String & getFilterName() const { return filter_name; }

    void setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_);

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    String filter_column_name;
    DataTypePtr filter_column_type;
    /// Stable structural id (`_runtime_filter_<hash>`), shown in EXPLAIN and serialized, so the build
    /// step and its matching `__applyFilter` carry the same visible id.
    String filter_name;
    /// Random per-plan-build key the built filter is registered under in the `IRuntimeFilterLookup`;
    /// the matching `__applyFilter` looks it up by the same key. Kept off the plan (not shown, not
    /// serialized) so it never enters a plan-step hash. Empty for a deserialized step (then inert).
    String filter_key;

    UInt64 exact_values_limit;
    UInt64 bloom_filter_bytes;
    UInt64 bloom_filter_hash_functions;
    Float64 pass_ratio_threshold_for_disabling;
    UInt64 blocks_to_skip_before_reenabling;
    Float64 max_ratio_of_set_bits_in_bloom_filter;

    bool allow_to_use_not_exact_filter;

    /// Measured distinct build-side keys from prior statistics, used to choose the bloom filter size.
    std::optional<UInt64> distinct_keys_hint;
};

}
