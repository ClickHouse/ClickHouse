#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct FilterDAGOutputPruningResult
{
    bool changed = false;
    bool input_positions_changed = false;
    std::vector<size_t> required_input_positions;
};

/// What pruneFilterDAGOutputsByPosition would do, computed without touching the DAG.
struct FilterDAGOutputPruningPlan
{
    /// Whether inputs may be removed, and the value the filter column flag takes.
    bool remove_inputs = false;
    bool remove_filter_column = false;
    /// DAG output positions to keep, before the filter column is erased from the header, the filter
    /// column included: it is needed to filter, whether or not anyone reads it.
    ///
    /// Unlike the other two steps, these are not the caller's positions over again. The header the
    /// caller counts in has the filter column erased from it, so its positions are shifted back over
    /// that column first, and the filter column is then added whether or not it was asked for.
    std::vector<size_t> required_dag_positions;
    /// One entry per column of the input header, in header order.
    std::vector<IQueryPlanStep::InputColumn> input_columns;
    /// Whether the output header changes - a DAG output goes away, or the filter column is dropped from
    /// it now - and whether removeUnusedActions would erase any node.
    bool changes_output_header = false;
    bool removes_any_action = false;
    /// Whether the filter predicate folds to a constant through `materialize` once the filter column is
    /// dropped; the rest of the plan is worked out on the folded DAG.
    bool fold_filter_predicate = false;
    /// The position of the filter column in the DAG's outputs, before any is removed.
    size_t filter_output_position = 0;

    /// Input header positions of the pass-through columns nobody asked for.
    std::vector<size_t> droppedPassThroughPositions() const;
    FilterDAGOutputPruningResult toResult() const;
};

FilterDAGOutputPruningPlan analyzeFilterDAGOutputPruning(
    const ActionsDAG & dag,
    const String & filter_column_name,
    bool remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & required_output_positions,
    bool remove_inputs);

void applyFilterDAGOutputPruning(
    ActionsDAG & dag,
    bool & remove_filter_column,
    const Block & input_header,
    const FilterDAGOutputPruningPlan & plan);

/// Prune filter DAG outputs by position and return the input positions needed to compute the remaining
/// outputs and filter. The analysis above plus its application.
FilterDAGOutputPruningResult pruneFilterDAGOutputsByPosition(
    ActionsDAG & dag,
    const String & filter_column_name,
    bool & remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & required_output_positions,
    bool remove_inputs);

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
        , prevent_input_removal(other.prevent_input_removal)
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
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;

    bool canGetRequiredColumns() const override { return true; }
    RemoveUnusedColumnsResult getRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const override;
    bool canRemoveColumnsFromOutput() const override;

    void setPreventInputRemoval() { prevent_input_removal = true; }
    bool isInputRemovalPrevented() const { return prevent_input_removal; }

    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override;

    /// Everything removeUnusedColumns needs to know, computed without touching the step. Shared by
    /// removeUnusedColumns and getRequiredColumns so their answers cannot differ.
    struct RequiredColumnsPlan
    {
        RemoveUnusedColumnsResult result;
        FilterDAGOutputPruningPlan pruning;
    };

    RequiredColumnsPlan analyzeRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const;

    ActionsDAG actions_dag;
    String filter_column_name;
    bool remove_filter_column;
    bool prevent_input_removal = false;

    std::optional<std::pair<UInt64, String>> condition; /// for query condition cache
};

}
