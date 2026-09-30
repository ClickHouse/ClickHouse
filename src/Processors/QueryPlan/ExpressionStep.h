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
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs) override;

    bool canGetUnneededColumns() const override { return true; }
    UnneededInputPositions getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const override;

private:
    void updateOutputHeader() override;

    /// Everything removeUnusedColumns needs to know, computed without touching the step. Shared by
    /// removeUnusedColumns and getUnneededColumns so their answers cannot differ.
    struct UnneededColumnsPlan
    {
        /// The positions nobody needs, which are also the outputs that go away.
        std::vector<size_t> unneeded_output_positions;
        /// How many of them index the DAG's outputs. The output header holds those first and the
        /// pass-through columns after, and the positions are sorted, so the unneeded DAG outputs are a
        /// prefix of the positions rather than a list of their own.
        size_t unneeded_dag_position_count = 0;
        /// One entry per column of the input header, in header order.
        std::vector<InputColumnUsage> input_columns;

        /// The DAG outputs that remain, in their order.
        ActionsDAG::NodeRawConstPtrs neededDAGOutputs(const ActionsDAG::NodeRawConstPtrs & outputs) const;

        /// What the step does not need of its child: the columns it neither reads nor passes on.
        std::vector<size_t> unneededInputPositions() const;
    };

    UnneededColumnsPlan analyzeUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const;

    ActionsDAG actions_dag;
};

}
