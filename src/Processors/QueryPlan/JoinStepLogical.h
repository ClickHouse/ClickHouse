#pragma once

#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Core/Joins.h>
#include <Interpreters/JoinExpressionActions.h>

namespace DB
{

class StorageJoin;
class IKeyValueEntity;
struct JoinAlgorithmParams;
struct StorageID;

struct PreparedJoinStorage
{
    std::unordered_map<String, String> column_mapping;

    /// At most one of these fields is set
    std::shared_ptr<StorageJoin> storage_join;
    std::shared_ptr<const IKeyValueEntity> storage_key_value;

    operator bool() const { return storage_join || storage_key_value; } /// NOLINT

    template <typename Visitor>
    void visit(Visitor && visitor)
    {
        if (storage_join)
            visitor(storage_join);
        else if (storage_key_value)
            visitor(storage_key_value);
    }
};


/** JoinStepLogical is a logical step for JOIN operation.
  * Doesn't contain any specific join algorithm or other execution details.
  * It's place holder for join operation with it's description that can be serialized.
  * Transformed to actual join step during plan optimization.
  */
class JoinStepLogical final : public IQueryPlanStep
{
public:
    JoinStepLogical(
        SharedHeader left_header_,
        SharedHeader right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        const NameSet & required_output_columns_,
        const std::unordered_map<String, const ActionsDAG::Node *> & changed_types,
        bool use_nulls_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    JoinStepLogical(
        const SharedHeader & left_header_,
        const SharedHeader & right_header_,
        JoinOperator join_operator_,
        JoinExpressionActions join_expression_actions_,
        std::vector<const ActionsDAG::Node *> actions_after_join_,
        JoinSettings join_settings_,
        SortingStep::Settings sorting_settings_);

    ~JoinStepLogical() override;

    String getName() const override { return "JoinLogical"; }
    String getSerializationName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const SortingStep::Settings & getSortingSettings() const { return sorting_settings; }
    const JoinSettings & getJoinSettings() const { return join_settings; }
    JoinSettings & getJoinSettings() { return join_settings; }
    const JoinOperator & getJoinOperator() const { return join_operator; }
    JoinOperator & getJoinOperator() { return join_operator; }

    const ActionsDAG & getActionsDAG() const { return *expression_actions.getActionsDAG(); }

    std::vector<JoinActionRef> getInputActions() const;
    std::vector<JoinActionRef> getOutputActions() const;

    std::pair<JoinExpressionActions, JoinOperator> detachExpressions()
    {
        return {std::move(expression_actions), std::move(join_operator)};
    }

    const JoinSettings & getSettings() const { return join_settings; }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `JoinStepLogical` and
    /// `IQueryPlanStep`. Reachability is checked against `buildPhysicalJoin`, which is what a logical
    /// join is turned into (this step has no `transformPipeline`; `updatePipeline` throws), and against
    /// the optimizer passes that test the step directly.
    ///
    /// Own fields:
    ///  - `expression_actions` - on the wire: `serialize` writes the whole `ActionsDAG`. Its
    ///    `expression_sources` companion map is derived - `deserialize` rebuilds it from the DAG's
    ///    inputs and the two input headers, and `swapInputs` / `resetNodeSources` keep the two in
    ///    step; the non-input entries are lazily folded from the children.
    ///  - `join_operator` - `kind`, `strictness`, `locality`, `expression` and `residual_filter` are on
    ///    the wire (`JoinOperator::serialize` writes the enums and the two node lists as DAG node
    ///    ids). `shared_runtime_filter_descriptors` is **extras**: nothing serializes it, and the
    ///    `joinRuntimeFilter` pass sets it so that `HashJoin` publishes those shared runtime filters -
    ///    the readers of those filters prune rows.
    ///  - `actions_after_join` - on the wire (`serialize` writes it as a DAG node id list).
    ///  - `join_settings` - on the wire (`JoinSettings::updatePlanSettings`) except four members:
    ///     - `join_analyze_mode` - **extras**. `updatePlanSettings` does not write it at all, and
    ///       `MergeJoinTransform` and `MatchedRowsStats` branch on it. In practice every construction
    ///       site takes it from the query context, so it is uniform within a plan; encoded anyway
    ///       rather than relying on that.
    ///     - `max_block_size`, `temporary_files_codec`, `temporary_files_buffer_size` - **extras**.
    ///       `JoinSettings::updatePlanSettings` does assign all three, but `serializeSettings` runs it
    ///       first and `sorting_settings.updatePlanSettings` second, and
    ///       `SortingStep::Settings::updatePlanSettings` assigns the same three plan-setting names.
    ///       `QueryPlanSerializationSettings` is a keyed map, so the later write wins: only the
    ///       sorting values reach the wire and the join's three are dropped (which is also why
    ///       `deserialize` reconstructs `JoinSettings` from the sorting values). Encoded fail-closed:
    ///       the join algorithms read `max_block_size` and the two temporary-file settings when they
    ///       spill, nothing forces the two settings structs to agree, and per-query uniformity is a
    ///       property of today's construction sites rather than an invariant. Note that the dropped
    ///       `join_settings.temporary_files_buffer_size` is still *validated* on assignment, which is
    ///       why `supportsCascadesIdentity()` below has to check it for zero.
    ///  - `sorting_settings` - on the wire the same way `SortingStep` has it
    ///    (`Settings::updatePlanSettings`), except: `max_bytes_in_query_before_external_sort`, derived
    ///    from the wire-covered ratio and the machine's memory, excluded; and
    ///    `read_in_order_use_buffering` / `read_in_order_use_virtual_row_per_block`, which are
    ///    **extras** because `updatePlanSettings` writes neither and these settings are handed to the
    ///    sorting steps the physical join builds.
    ///  - `optimized` - **extras**. Load-bearing: it is what `optimizeJoin` tests to decide whether a
    ///    join may be reordered, and correlated-subquery decorrelation pins the layout of its result
    ///    join through it (see the comment in `clone`) because only that layout guarantees the
    ///    in-memory buffer of the common subplan is fully written before it is read.
    ///  - `result_rows_estimation` - **extras**. Not just display: the Cascades
    ///    `StatisticsDerivation` prefers it over its own derivation, so it feeds the cost model, and
    ///    `optimizeJoin` reuses it as the statistics of an already-optimized sub-join.
    ///  - `result_column_stats` - **extras**, for the same reason (`optimizeJoin` reads it next to
    ///    `result_rows_estimation`). Encoded sorted by column name, since the map's own iteration
    ///    order is not part of its value.
    ///  - `imprecise_estimate` - **extras**, read together with the two above by `optimizeJoin`.
    ///    Follow-up for the three estimation fields above: they are cost-only, and they are in the
    ///    extras fail-closed. Once a join-rebuild path that clears estimates lands (the feature
    ///    branch's `rebuildJoinWithNewInput` in `AggregationPushdown`), they must be revisited -
    ///    otherwise a rebuilt join will never deduplicate against an ingested one.
    ///  - `right_hash_table_cache_key` - **extras**. Read by `buildPhysicalJoin` (it becomes the
    ///    `JoinAlgorithmParams` hash-table key and the `StatsCollectingParams` key that seeds the
    ///    right-side size estimate) and by `joinRuntimeFilter`.
    ///  - `left_relation`, `right_relation` (`RelationEstimateInfo`) - `estimated_rows` of both sides
    ///    is **extras**: `buildPhysicalJoin` copies the right side's into
    ///    `JoinAlgorithmParams::rhs_size_estimation`, which picks the join algorithm, and
    ///    `joinRuntimeFilter` reads the left side's through `getInputRowsEstimation`. The other
    ///    members - `name`, `source`, `imprecise_estimate`, `composite` - are excluded: they are read
    ///    only by `displayName` / `getReadableRelationName`, i.e. the EXPLAIN label.
    ///  - `table_stats_hint` - **extras**. `optimizeJoin` feeds it to the query-graph builder as
    ///    `stats_hint`, which replaces the relation estimates with synthetic ones.
    ///  - `join_algorithm_params` - excluded: a build-time cache. `buildPhysicalJoin` is its only
    ///    writer and it fills it only when null, from `join_settings`, the optimization settings and
    ///    the two extras above; `clone` does not copy it, so it is null on every step the optimizer
    ///    sees.
    ///  - `tmp_volume`, `tmp_data` - excluded: dead members. The class is `final` and no code in
    ///    `JoinStepLogical.cpp` reads or writes either of them.
    ///  - `disjunctions_optimization_applied` - **extras**. Not on the wire; `filterPushDown` refuses
    ///    to push a filter through a join that has it set, so the two are not interchangeable.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: the DAG on the wire carries its own inputs with names
    ///    and types, `expression_sources` is rebuilt from them, and which side each input belongs to
    ///    follows from the ordered child groups that `GroupExpression::globallyEqualTo` compares
    ///    separately (`swapInputs` swaps the headers and the sources together).
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    ///
    /// `isSerializable()` is unconditionally `true`, but a correlated `PLACEHOLDER` node makes
    /// `ActionsDAG::serialize` throw, so the predicate also requires `!hasCorrelatedExpressions()`.
    /// The node-list writers cannot throw here: `getNodeToIdMap` maps every node of the DAG, and the
    /// three lists hold pointers into that DAG. `serializeSettings` can throw as well: it assigns
    /// three `NonZeroUInt64` plan settings (`grace_hash_join_initial_buckets`,
    /// `grace_hash_join_max_buckets` and `temporary_files_buffer_size`, the last one from both
    /// settings structs), and a zero value there throws `BAD_ARGUMENTS`.
    bool supportsCascadesIdentity() const override;
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override
    {
        return expression_actions.getActionsDAG()->hasCorrelatedColumns();
    }

    void addConditions(ActionsDAG actions_dag);

    /// Extract the part of the JOIN ON expression that can be evaluated on `side` alone, to be applied
    /// as a filter on that input.
    std::optional<ActionsDAG::ActionsForFilterPushDown> getFilterActions(
        JoinTableSide side, const SharedHeader & left_header, const SharedHeader & right_header);

    struct ActionsDAGWithKeys
    {
        ActionsDAG actions_dag;
        ActionsDAG::NodeRawConstPtrs keys;
    };

    std::optional<std::pair<ActionsDAGWithKeys, ActionsDAGWithKeys>>
    preCalculateKeys(const SharedHeader & left_header, const SharedHeader & right_header);

    static void buildPhysicalJoin(
        QueryPlan::Node & node,
        const QueryPlanOptimizationSettings & optimization_settings,
        QueryPlan::Nodes & nodes);

    std::unordered_set<JoinTableSide> typeChangingSides() const;

    bool isOptimized() const { return optimized; }
    std::optional<UInt64> getResultRowsEstimation() const { return result_rows_estimation; }
    bool hasImpreciseEstimate() const { return imprecise_estimate; }
    const std::unordered_map<String, ColumnStats> & getResultColumnStats() const { return result_column_stats; }
    std::optional<UInt64> getInputRowsEstimation(JoinTableSide side) const;

    void setOptimized(
        std::optional<UInt64> estimated_rows_ = {},
        std::unordered_map<String, ColumnStats> column_stats_ = {},
        bool imprecise_estimate_ = false)
    {
        optimized = true;
        result_rows_estimation = estimated_rows_;
        result_column_stats = std::move(column_stats_);
        imprecise_estimate = imprecise_estimate_;
    }

    void setInputLabels(String left_table_label_, String right_table_label_)
    {
        left_relation = RelationEstimateInfo{.name = std::move(left_table_label_)};
        right_relation = RelationEstimateInfo{.name = std::move(right_table_label_)};
    }

    void setInputRelations(RelationEstimateInfo left_relation_, RelationEstimateInfo right_relation_)
    {
        left_relation = std::move(left_relation_);
        right_relation = std::move(right_relation_);
    }

    std::pair<std::reference_wrapper<const String>, std::reference_wrapper<const String>> getInputLabels() const
    {
        return {std::cref(left_relation.name), std::cref(right_relation.name)};
    }

    String getReadableRelationName() const;

    ActionsDAG::NodeRawConstPtrs getActionsAfterJoin() const { return actions_after_join; }

    std::string_view getTableStatsHint() const { return table_stats_hint; }
    void setTableStatsHint(String table_stats_hint_) { table_stats_hint = std::move(table_stats_hint_); }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool isDisjunctionsOptimizationApplied() const { return disjunctions_optimization_applied; }
    void setDisjunctionsOptimizationApplied(bool v) { disjunctions_optimization_applied = v; }

    /// Swap left and right sides
    void swapInputs();

    UInt64 getRightHashTableCacheKey() const { return right_hash_table_cache_key; }
    void setRightHashTableCacheKey(UInt64 right_hash_table_cache_key_) { right_hash_table_cache_key = right_hash_table_cache_key_; }

protected:
    SharedHeader calculateOutputHeader(const NameSet & required_output_columns_set) const;
    void updateOutputHeader() override;

    bool isDummyColumnOfThisStep(const ActionsDAG::Node * node) const;

    std::vector<std::pair<String, String>> describeJoinProperties() const;

    JoinExpressionActions expression_actions;
    JoinOperator join_operator;

    /// These are the nodes which are used to split expressions calculated before and after join
    /// Nodes from this list are used as inputs for ActionsDAG executed after join operation
    /// It can be input or node with toNullable function applied to input
    ActionsDAG::NodeRawConstPtrs actions_after_join = {};

    JoinSettings join_settings;
    SortingStep::Settings sorting_settings;

    /// Runtime info, do not serialize

    bool optimized = false;
    std::optional<UInt64> result_rows_estimation = {};
    std::unordered_map<String, ColumnStats> result_column_stats = {};

    /// True when the row count estimation used by join reordering was derived from the primary index
    /// rather than column statistics (because `use_statistics` is enabled but statistics are missing).
    bool imprecise_estimate = false;
    UInt64 right_hash_table_cache_key = 0;

    RelationEstimateInfo left_relation;
    RelationEstimateInfo right_relation;

    /// Table statistics hint passed via query parameter, consumed by the Cascades optimizer.
    String table_stats_hint;


    std::unique_ptr<JoinAlgorithmParams> join_algorithm_params;
    VolumePtr tmp_volume;
    TemporaryDataOnDiskScopePtr tmp_data;

private:

    bool disjunctions_optimization_applied = false;
};


class JoinStepLogicalLookup final : public ISourceStep
{
public:
    JoinStepLogicalLookup(QueryPlan child_plan_, PreparedJoinStorage prepared_join_storage_, bool use_nulls_);

    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;
    String getName() const override { return "JoinStepLogicalLookup"; }

    QueryPlanRawPtrs getChildPlans() override;

    PreparedJoinStorage & getPreparedJoinStorage() { return prepared_join_storage; }

    bool useNulls() const { return use_nulls; }

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);
private:
    PreparedJoinStorage prepared_join_storage;
    QueryPlan child_plan;

    bool use_nulls = false;
    bool optimized = false;
};

std::string_view joinTypePretty(JoinKind join_kind, JoinStrictness strictness);

/// Whether the IEJoin algorithm is preferred for this join: `ie_join` is listed first in
/// `join_algorithm` and the ON expression has two inequality conditions the operator can take.
/// For optimization passes that would otherwise claim the join for a hash-family algorithm
/// (e.g. runtime filters). The condition eligibility is the same one the conversion to the
/// physical step applies, so `true` means IEJoin takes the join unless the right side is a
/// prepared `Join` storage (which those passes exclude on their own).
bool isIEJoinPreferred(const JoinOperator & join_operator, const JoinSettings & join_settings);


}
