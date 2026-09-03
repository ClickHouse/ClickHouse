#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>

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
        RuntimeFilterGeometry geometry_,
        bool allow_to_use_not_exact_filter_,
        bool track_key_range_,
        std::optional<UInt64> distinct_keys_hint_ = std::nullopt);

    BuildRuntimeFilterStep(const BuildRuntimeFilterStep & other) = default;

    String getName() const override { return "BuildRuntimeFilter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    const String & getFilterColumnName() const { return filter_column_name; }
    const String & getFilterName() const { return filter_name; }
    const String & getFilterKey() const { return filter_key; }
    /// Only for restoring a deserialized step from a sibling `__applyFilter` in the same fragment.
    void setFilterKey(String filter_key_)
    {
        chassert(filter_key.empty());
        filter_key = std::move(filter_key_);
    }
    const DataTypePtr & getFilterColumnType() const { return filter_column_type; }
    bool allowsNotExactFilter() const { return allow_to_use_not_exact_filter; }
    const RuntimeFilterGeometry & getGeometry() const { return geometry; }
    void setGeometry(const RuntimeFilterGeometry & geometry_) { geometry = geometry_; }

    struct FilterExchange
    {
        String exchange_id;
        Strings destination_buckets;
    };

    struct TreeExchange
    {
        String exchange_id;
        /// Ordered buckets of the build stage; a task's parent is `own index / fan_in`.
        Strings source_buckets;
        size_t fan_in = 0;
    };

    /// Destinations are the consuming-stage task buckets. One filter may be applied in several
    /// stages, so the partials go out over one exchange per receiving stage. Used when the build
    /// stage has a single task (the task is then the root of the merge tree and broadcasts directly).
    void addExchange(String exchange_id_, Strings destination_buckets_);

    /// The build stage has several tasks and the partials go through a merge tree: each build task
    /// sends its partial once, to its parent merge task, computed from the task's position in
    /// `source_buckets` (the ordered buckets of the build stage) as `index / fan_in`. Mutually
    /// exclusive with `addExchange`.
    void setTreeExchange(String exchange_id_, Strings source_buckets_, size_t fan_in_);

    bool hasFilterExchanges() const { return !exchanges.empty() || tree_exchange; }

    void setEstimatedBuildRows(std::optional<UInt64> estimated_build_rows_) { estimated_build_rows = estimated_build_rows_; }
    std::optional<UInt64> getEstimatedBuildRows() const { return estimated_build_rows; }

    void setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;
    void transformPipelineForTransport(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings);

    String filter_column_name;
    DataTypePtr filter_column_type;
    /// Stable structural id (`_runtime_filter_<hash>`), shown in EXPLAIN and serialized, so the build
    /// step and its matching `__applyFilter` carry the same visible id.
    String filter_name;
    /// Random per-plan-build key the built filter is registered under in the `IRuntimeFilterLookup`;
    /// the matching `__applyFilter` looks it up by the same key. Kept off the plan (not shown, not
    /// serialized) so it never enters a plan-step hash. After deserialize it is restored from a
    /// sibling `__applyFilter` in the same fragment.
    String filter_key;

    RuntimeFilterGeometry geometry;

    bool allow_to_use_not_exact_filter;
    /// Record the key values/range for left-side index analysis; off avoids an extra build-side scan.
    bool track_key_range;

    /// Measured distinct build-side keys from prior statistics, used to choose the bloom filter size.
    std::optional<UInt64> distinct_keys_hint;

    /// Both empty: local build mode (register in this task's lookup). The distributed split assigns
    /// the filter exchange(s) afterwards; then exactly one of the two is set.
    std::vector<FilterExchange> exchanges;
    std::optional<TreeExchange> tree_exchange;

    /// Row estimate stamped before the plan is cut; consumed only by the initiator when sizing the
    /// exact phase. Not serialized.
    std::optional<UInt64> estimated_build_rows;
};

}
