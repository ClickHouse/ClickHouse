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

/// Prune filter DAG outputs by position and return the input positions needed to compute the remaining outputs and filter.
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

    static bool canUseType(const DataTypePtr & type);

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `FilterStep`,
    /// `ITransformingStep` and `IQueryPlanStep`:
    ///  - on the wire (written by `serialize`): `actions_dag`, `filter_column_name`,
    ///    `remove_filter_column`.
    ///  - covered by the identity encoding itself: `output_header`.
    ///  - extras: `prevent_input_removal` - not on the wire, and it blocks later input pruning,
    ///    same as in `ExpressionStep`. `condition` - not on the wire; when set,
    ///    `transformPipeline` wires `FilterTransform` to a `QueryConditionCache` and writes ranges
    ///    keyed by `condition->first`/`condition->second` at runtime, so a step with `condition`
    ///    set is not interchangeable with one without, or with a different hash/text.
    ///  - derived: `input_headers` - `actions_dag` carries its own inputs' names and types like in
    ///    `ExpressionStep`, and the pass-through columns are exactly the tail of `output_header`
    ///    (see `FilterTransform::transformHeader`) once `filter_column_name` is optionally erased
    ///    (already on the wire); only the input column order is not recoverable, and it does not
    ///    constrain execution because `ExpressionActions` resolves columns by name.
    ///    `transform_traits` and `data_stream_traits` - computed from `getTraits` at construction
    ///    and never mutated. `collect_processors` - always default for this step.
    ///  - display or runtime instrumentation only: `step_description`, `step_index`, `processors`,
    ///    `dataflow_cache_updater`.
    ///
    /// `isSerializable()` is unconditionally `true`, but a correlated `PLACEHOLDER` node makes
    /// `actions_dag.serialize` throw ("Unknown node type"), so the predicate also requires
    /// `!hasCorrelatedExpressions()` to keep the invariant true by construction.
    bool supportsCascadesIdentity() const override { return isSerializable() && !hasCorrelatedExpressions(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }
    void decorrelateActions() { actions_dag.decorrelate(); }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    void setPreventInputRemoval() { prevent_input_removal = true; }
    bool isInputRemovalPrevented() const { return prevent_input_removal; }

    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    String filter_column_name;
    bool remove_filter_column;
    bool prevent_input_removal = false;

    std::optional<std::pair<UInt64, String>> condition; /// for query condition cache
};

}
