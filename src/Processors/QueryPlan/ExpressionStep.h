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
    {}

    [[nodiscard]] String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    ActionsDAG & getExpression() { return actions_dag; }
    [[nodiscard]] const ActionsDAG & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void serialize(Serialization & ctx) const override;
    [[nodiscard]] bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    [[nodiscard]] QueryPlanStepPtr clone() const override;

    [[nodiscard]] bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }
    void decorrelateActions() { actions_dag.decorrelate(); }

    [[nodiscard]] bool supportsDataflowStatisticsCollection() const override { return true; }

    [[nodiscard]] bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs) override;

    [[nodiscard]] UnneededInputPositions getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const override;

private:
    void updateOutputHeader() override;

    /// Everything removeUnusedColumns needs to know, computed without touching the step.
    /// Shared by removeUnusedColumns and getUnneededColumns so their answers cannot differ.
    struct UnneededColumnsPlan
    {
        /// One entry per column of the input header, in header order.
        std::vector<InputColumnUsage> input_columns;
        /// The positions nobody needs that the step drops, which are the outputs that go away. A pass-through
        /// column after a needed one of the same name is kept, even if nobody needs it.
        std::vector<size_t> unneeded_output_positions;
        /// How many of them index the DAG's outputs. The output header holds the DAG outputs first and the
        /// pass-through columns after, so the unneeded DAG outputs are a prefix of `unneeded_output_positions`.
        size_t unneeded_dag_position_count = 0;

        /// The DAG outputs that remain, in their order.
        [[nodiscard]] ActionsDAG::NodeRawConstPtrs neededDAGOutputs(const ActionsDAG::NodeRawConstPtrs & outputs) const;

        /// What the step does not need of its child: the columns it neither reads nor passes on.
        [[nodiscard]] std::vector<size_t> unneededInputPositions() const;
    };

    [[nodiscard]] UnneededColumnsPlan analyzeUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const;

    ActionsDAG actions_dag;
};

}
