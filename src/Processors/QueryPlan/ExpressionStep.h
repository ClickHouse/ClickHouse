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
        , parallelize_single_stream(other.parallelize_single_stream)
    {}

    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    ActionsDAG & getExpression() { return actions_dag; }
    const ActionsDAG & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

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

    /// `transformPipeline` decides on its own whether the parallel evaluation is also safe,
    /// so this may be set on any expression.
    void setParallelizeSingleStream() { parallelize_single_stream = true; }
    bool isSingleStreamParallelized() const { return parallelize_single_stream; }

private:
    void updateOutputHeader() override;

    ActionsDAG actions_dag;
    bool prevent_input_removal = false;
    bool parallelize_single_stream = false;
};

}
