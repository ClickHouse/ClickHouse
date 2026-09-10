#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

#include <optional>

namespace DB
{

struct BuildRuntimeFilterWire;

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
        bool track_key_range_,
        std::optional<UInt64> distinct_keys_hint_ = std::nullopt,
        bool distinct_keys_hint_matches_filter_key_ = false);

    BuildRuntimeFilterStep(const BuildRuntimeFilterStep & other) = default;

    String getName() const override { return "BuildRuntimeFilter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterColumnName() const { return filter_column_name; }
    const String & getFilterName() const { return filter_name; }

    void setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `BuildRuntimeFilterStep.cpp` declares.
    BuildRuntimeFilterWire toWire() const;
    static QueryPlanStepPtr fromWire(BuildRuntimeFilterWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
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
    /// Record the key values/range for left-side index analysis; off avoids an extra build-side scan.
    bool track_key_range;

    /// Measured distinct build-side keys from prior statistics, used to choose the bloom filter size.
    std::optional<UInt64> distinct_keys_hint;
    /// Whether the filter key is the whole join key, so that the hint counts this filter's distinct keys.
    bool distinct_keys_hint_matches_filter_key;
};

/// What `BuildRuntimeFilterStep` puts on the wire in the framed format. The six limits travel
/// through the settings channel. The rendezvous key, the key-range tracking and the distinct-keys
/// hint stay local: runtime filters are derived again per plan build, and a step built from the
/// wire is inert.
struct BuildRuntimeFilterWire
{
    String filter_column_name;
    /// The data type by name.
    String filter_column_type;
    String filter_name;
    bool allow_to_use_not_exact_filter = false;

    UInt64 exact_values_limit = 10000;
    UInt64 bloom_filter_bytes = 512 * 1024;
    UInt64 bloom_filter_hash_functions = 3;
    Float64 pass_ratio_threshold_for_disabling = 0.7;
    UInt64 blocks_to_skip_before_reenabling = 30;
    Float64 max_ratio_of_set_bits_in_bloom_filter = 0.7;

    bool operator==(const BuildRuntimeFilterWire &) const = default;
};

}
