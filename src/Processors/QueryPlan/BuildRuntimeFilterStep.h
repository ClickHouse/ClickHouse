#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/RuntimeFilterBuildOptions.h>

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
        RuntimeFilterBuildOptions build_options_);

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
    bool allowsNotExactFilter() const { return build_options.polarity == RuntimeFilterPolarity::Contains; }
    const RuntimeFilterGeometry & getGeometry() const { return build_options.geometry; }
    void setGeometry(const RuntimeFilterGeometry & geometry_) { build_options.geometry = geometry_; }

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

    /// Sends the partial directly to the consuming-stage task buckets, over one exchange per receiving
    /// stage. `wireRuntimeFilterExchangeTopology` does not use it: every transported filter goes through a
    /// merge stage (`setTreeExchange`), even from a single build task.
    void addExchange(String exchange_id_, Strings destination_buckets_);

    /// Sends the partial through a merge tree: each build task sends it once, to its parent merge task
    /// (see `TreeExchange::source_buckets`). Mutually exclusive with `addExchange`.
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

    RuntimeFilterBuildOptions build_options;

    /// Both empty: local build mode (register in this task's lookup). The distributed split assigns
    /// the filter exchange(s) afterwards; then exactly one of the two is set.
    std::vector<FilterExchange> exchanges;
    std::optional<TreeExchange> tree_exchange;

    /// Row estimate stamped before the plan is cut; consumed only by the initiator when sizing the
    /// exact phase. Not serialized.
    std::optional<UInt64> estimated_build_rows;
};

}
