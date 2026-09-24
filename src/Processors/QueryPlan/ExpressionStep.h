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
    RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) override;
    bool canRemoveColumnsFromOutput() const override;

    bool canGetRequiredColumns() const override { return true; }
    RemoveUnusedColumnsResult getRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const override;

    /// Prevent future input removal by removeUnusedColumns.
    /// Used when extra columns were absorbed from a child step that cannot reduce its output
    /// (e.g., ReadFromMergeTree with FINAL must keep sort key columns).
    void setPreventInputRemoval() { prevent_input_removal = true; }
    bool isInputRemovalPrevented() const { return prevent_input_removal; }

private:
    void updateOutputHeader() override;

    /// What one column of the step's input header is to the step once the unused columns are gone.
    enum class InputColumn : uint8_t
    {
        ReadAndNeeded,      /// an input reads it, and what that input feeds is still needed
        ReadNotNeeded,      /// an input reads it, and nothing needs that input any more
        PassesThroughNeeded, /// no input reads it, and the caller asked for the column itself
        PassesThroughDropped, /// no input reads it, and nobody asked for it
    };

    /// Everything removeUnusedColumns needs to know, computed without touching the step. Shared by
    /// removeUnusedColumns and getRequiredColumns so their answers cannot differ.
    ///
    /// The analysis behind it asks the DAG alone; `remove_inputs` says what to make of the answer, and
    /// is applied when this is turned into a `RemoveUnusedColumnsResult`.
    struct RequiredColumnsPlan
    {
        /// Whether inputs may be removed, after the prevent_input_removal override.
        bool remove_inputs = false;
        /// The positions the caller asked for, which are also the outputs that survive.
        std::vector<size_t> required_output_positions;
        /// How many of them index the DAG's outputs. The output header holds those first and the
        /// pass-through columns after, and the positions are sorted, so the DAG outputs asked for are a
        /// prefix of the positions rather than a list of their own.
        size_t dag_position_count = 0;
        /// One entry per column of the input header, in header order.
        std::vector<InputColumn> input_columns;
        /// Whether any output goes away, and whether removeUnusedActions would erase any node. The
        /// second answer depends on `remove_inputs`, since an unread input is only erased when inputs
        /// may go, so both answers are kept.
        bool removes_any_output = false;
        bool removes_any_action_keeping_inputs = false;
        bool removes_any_action_removing_inputs = false;

        /// The DAG outputs the caller asked for, as positions in `getOutputs()`.
        std::span<const size_t> requiredDAGPositions() const
        {
            return {required_output_positions.data(), dag_position_count};
        }

        bool removesAnyAction() const;
        bool changesAnything() const;
        /// Input header positions of the pass-through columns nobody asked for. The apply step turns
        /// these into DAG inputs when the inputs themselves may not be removed.
        std::vector<size_t> droppedPassThroughPositions() const;
        RemoveUnusedColumnsResult toResult() const;
    };

    RequiredColumnsPlan analyzeRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const;

    ActionsDAG actions_dag;
    bool prevent_input_removal = false;
};

}
