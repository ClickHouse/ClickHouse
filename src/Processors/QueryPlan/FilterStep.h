#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// What `FilterStep::pruneDAGOutputsByPosition` did.
struct FilterDAGOutputPruningResult
{
    bool changed = false;
    bool input_positions_changed = false;
    std::vector<size_t> required_input_positions;
};

/// Implements WHERE, HAVING operations. See FilterTransform.
class FilterStep : public ITransformingStep
{
public:
    FilterStep(
        const SharedHeader & input_header_,
        ActionsDAG actions_dag_,
        String filter_column_name_,
        bool remove_filter_column_);

    FilterStep(const FilterStep & other)
        : ITransformingStep(other)
        , actions_dag(other.actions_dag.clone())
        , filter_column_name(other.filter_column_name)
        , remove_filter_column(other.remove_filter_column)
        , condition(other.condition)
    {}

    String getName() const override { return "Filter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAG & getExpression() const { return actions_dag; }
    ActionsDAG & getExpression() { return actions_dag; }
    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

    void setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_);
    /// Forget a previously attached query condition cache key, e.g. when a later optimization pass
    /// discovers that the filter's verdict is no longer reusable across executions.
    void resetConditionForQueryConditionCache() { condition.reset(); }

    static bool canUseType(const DataTypePtr & type);

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }
    void decorrelateActions() { actions_dag.decorrelate(); }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs) override;

    UnneededInputPositions getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const override;

    bool supportsDataflowStatisticsCollection() const override { return true; }

    /// Prunes the outputs of a filter DAG to the columns at `required_output_positions` of its output header, and says
    /// which input positions the remaining outputs and the filter need. For a caller that counts what it keeps, as
    /// `ReadFromMergeTree` does for PREWHERE and the row policy filter.
    static FilterDAGOutputPruningResult pruneDAGOutputsByPosition(
        ActionsDAG & dag,
        const String & filter_column_name,
        bool & remove_filter_column,
        const Block & input_header,
        const std::vector<size_t> & required_output_positions);

private:
    void updateOutputHeader() override;

    /// Everything removeUnusedColumns needs to know, computed without touching the DAG.
    /// Shared by removeUnusedColumns, getUnneededColumns and pruneDAGOutputsByPosition so their answers cannot differ.
    struct UnneededColumnsPlan
    {
        /// One entry per column of the input header, in header order.
        std::vector<InputColumnUsage> input_columns;
        /// The DAG outputs nobody needs, as positions in `getOutputs` before any is removed, sorted.
        /// Never contains the filter column: it is needed to filter, whether or not anyone reads it.
        /// Unlike for the other steps, these are not just the caller's positions.
        /// The header the caller counts in may have the filter column erased from it,
        /// so the caller's positions are shifted back over that column first.
        std::vector<size_t> unneeded_dag_positions;
        /// The position of the filter column in the DAG's outputs, before any is removed.
        size_t filter_output_position = 0;

        /// Whether the filter column is removed from the output header after the pruning: it already was, or nobody
        /// reads it any more.
        bool remove_filter_column = false;

        /// Whether the output header changes, and whether removeUnusedActions would erase any node.
        bool changes_output_header = false;
        bool removes_any_action = false;
        /// Whether the filter predicate folds to a constant through `materialize` once the filter column is dropped;
        /// the rest of the plan is worked out on the folded DAG.
        bool fold_filter_predicate = false;

        /// The DAG outputs that remain, in their order.
        ActionsDAG::NodeRawConstPtrs neededDAGOutputs(const ActionsDAG::NodeRawConstPtrs & outputs) const;

        /// The part of the pruning that concerns the outputs: the fold of the predicate, the outputs that remain, and
        /// the filter column flag.
        void applyToOutputs(ActionsDAG & dag, bool & remove_filter_column_) const;

        FilterDAGOutputPruningResult toResult() const;
    };

    static UnneededColumnsPlan analyzeUnneededColumns(
        const ActionsDAG & dag,
        const String & filter_column_name,
        bool remove_filter_column,
        const Block & input_header,
        const std::vector<size_t> & unneeded_output_positions);

    UnneededColumnsPlan analyzeUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const;

    ActionsDAG actions_dag;
    String filter_column_name;
    bool remove_filter_column;

    std::optional<std::pair<UInt64, String>> condition; /// for query condition cache
};

}
