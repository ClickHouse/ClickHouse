#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:
    explicit ExpressionStep(SharedHeader input_header_, ActionsDAG actions_dag_);

    ExpressionStep(const ExpressionStep & other)
        : ITransformingStep(other)
        , actions_dag(other.actions_dag.clone())
        , prevent_input_removal(other.prevent_input_removal)
    {}

    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    ActionsDAG & getExpression() { return actions_dag; }
    const ActionsDAG & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `ExpressionStep`,
    /// `ITransformingStep` and `IQueryPlanStep`:
    ///  - on the wire (written by `serialize`): `actions_dag`.
    ///  - covered by the identity encoding itself: `output_header`.
    ///  - extras: `prevent_input_removal` - not on the wire, and it blocks later input pruning,
    ///    so two otherwise identical steps are not interchangeable.
    ///  - derived: `input_headers` - the DAG carries its own inputs' names and types, and the
    ///    pass-through columns are exactly the tail of `output_header` (see
    ///    `ExpressionTransform::transformHeader`); only the order of the input columns is not
    ///    recoverable, and it does not constrain execution because `ExpressionActions` resolves
    ///    columns by name. `transform_traits` and `data_stream_traits` - computed from
    ///    `actions_dag` by `getTraits` and never mutated for this step. `collect_processors` -
    ///    always default for this step.
    ///  - display or runtime instrumentation only: `step_description`, `step_index`,
    ///    `processors`, `dataflow_cache_updater` (only ever set on source reading steps).
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

    bool supportsDataflowStatisticsCollection() const override { return true; }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    /// Prevent future input removal by removeUnusedColumns.
    /// Used when extra columns were absorbed from a child step that cannot reduce its output
    /// (e.g., ReadFromMergeTree with FINAL must keep sort key columns).
    void setPreventInputRemoval() { prevent_input_removal = true; }
    bool isInputRemovalPrevented() const { return prevent_input_removal; }

private:
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    bool prevent_input_removal = false;
};

}
