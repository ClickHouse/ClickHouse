#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

#include <span>

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

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return actions_dag.hasCorrelatedColumns(); }
    void decorrelateActions() { actions_dag.decorrelate(); }

    bool supportsDataflowStatisticsCollection() const override { return true; }

    bool canRemoveUnusedColumns() const override;
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, const std::vector<PrunedInput> & inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool canGetRequiredColumns() const override { return true; }
    RequiredInputPositions getRequiredColumns(const std::vector<size_t> & required_output_positions) const override;

    /// Prevent future input removal by removeUnusedColumns.
    /// Used when extra columns were absorbed from a child step that cannot reduce its output
    /// (e.g., ReadFromMergeTree with FINAL must keep sort key columns).
    void setPreventInputRemoval() { prevent_input_removal = true; }
    bool isInputRemovalPrevented() const { return prevent_input_removal; }

private:
    void updateOutputHeader() override;

    /// Everything removeUnusedColumns needs to know, computed without touching the step. Shared by
    /// removeUnusedColumns and getRequiredColumns so their answers cannot differ.
    struct RequiredColumnsPlan
    {
        /// Whether inputs may be removed at all, which prevent_input_removal says they may not.
        bool remove_inputs = false;
        /// The positions the caller asked for, which are also the outputs that survive.
        std::vector<size_t> required_output_positions;
        /// How many of them index the DAG's outputs. The output header holds those first and the
        /// pass-through columns after, and the positions are sorted, so the DAG outputs asked for are a
        /// prefix of the positions rather than a list of their own.
        size_t dag_position_count = 0;
        /// One entry per column of the input header, in header order.
        std::vector<InputColumnUsage> input_columns;

        /// The DAG outputs the caller asked for, as positions in `getOutputs()`.
        std::span<const size_t> requiredDAGPositions() const
        {
            return {required_output_positions.data(), dag_position_count};
        }

        /// What the step needs of its child: the columns it reads and passes on, or all of them while
        /// inputs may not be removed.
        std::vector<size_t> requiredInputPositions() const;
    };

    RequiredColumnsPlan analyzeRequiredColumns(const std::vector<size_t> & required_output_positions) const;

    ActionsDAG actions_dag;
    bool prevent_input_removal = false;
};

}
