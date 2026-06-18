#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Chain of query actions steps. This class is needed to eliminate unnecessary actions calculations.
  * Each step is represented by actions DAG.
  *
  * Consider such example query:
  * SELECT expr(id) FROM test_table WHERE expr(id) > 0.
  *
  * We want to reuse expr(id) from previous expressions step, and not recalculate it in projection.
  * To do this we build a chain of all query action steps.
  * For example:
  * 1. Before where.
  * 2. Before order by.
  * 3. Projection.
  *
  * Initially root of chain is initialized with join tree query plan header.
  * Each next chain step, must be initialized with previous step available output columns.
  * That way we forward all available output columns (functions, columns, aliases) from first step of the chain to the
  * last step. After chain is build we can finalize it.
  *
  * Each step has input columns (some of them are not necessary) and output columns. Before chain finalize output columns
  * contain only necessary actions for step output calculation.
  * For each step starting from last (i), we add columns that are necessary for this step to previous step (i - 1),
  * and remove unused input columns of previous step(i - 1).
  * That way we reuse already calculated expressions from first step to last step.
  */

class ActionsChainStep;
using ActionsChainStepPtr = std::unique_ptr<ActionsChainStep>;
using ActionsChainSteps = std::vector<ActionsChainStepPtr>;

/// Actions chain step represent single step in actions chain.
class ActionsChainStep
{
public:
    /** Initialize actions step with actions dag.
      * Input column names initialized using actions dag nodes with INPUT type.
      * If use_actions_nodes_as_output_columns = true output columns are initialized using actions dag nodes.
      * If additional output columns are specified they are added to output columns.
      */
    explicit ActionsChainStep(ActionsAndProjectInputsFlagPtr actions_,
        bool use_actions_nodes_as_output_columns = true,
        ColumnsWithTypeAndName additional_output_columns_ = {});

    /// Get actions
    ActionsAndProjectInputsFlagPtr & getActions()
    {
        return actions;
    }

    /// Get actions
    const ActionsAndProjectInputsFlagPtr & getActions() const
    {
        return actions;
    }

    /// Get available output columns
    const ColumnsWithTypeAndName & getAvailableOutputColumns() const
    {
        return available_output_columns;
    }

    /// Get input column names
    const NameSet & getInputColumnNames() const
    {
        return input_columns_names;
    }

    /** Get child required output columns names.
      * Initialized during finalizeOutputColumns method call.
      */
    const NameSet & getChildRequiredOutputColumnsNames() const
    {
        return child_required_output_columns_names;
    }

    /** Finalize step output columns and remove unnecessary input columns.
      * If actions dag node has same name as child input column, it is added to actions output nodes.
      */
    void finalizeInputAndOutputColumns(const NameSet & child_input_columns);

    /// Dump step into buffer
    void dump(WriteBuffer & buffer) const;

    /// Dump step
    String dump() const;

private:
    void initialize();

    ActionsAndProjectInputsFlagPtr actions;

    bool use_actions_nodes_as_output_columns = true;

    NameSet input_columns_names;

    NameSet child_required_output_columns_names;

    ColumnsWithTypeAndName available_output_columns;

    ColumnsWithTypeAndName additional_output_columns;
};

/// Query actions chain
class ActionsChain
{
public:
    /// Add step into actions chain
    void addStep(ActionsChainStepPtr step)
    {
        steps.emplace_back(std::move(step));
    }

    /// Get steps
    const ActionsChainSteps & getSteps() const
    {
        return steps;
    }

    /// Get steps size
    size_t getStepsSize() const
    {
        return steps.size();
    }

    const ActionsChainStepPtr & at(size_t index) const
    {
        if (index >= steps.size())
            throw std::out_of_range("actions chain access is out of range");

        return steps[index];
    }

    ActionsChainStepPtr & at(size_t index)
    {
        if (index >= steps.size())
            throw std::out_of_range("actions chain access is out of range");

        return steps[index];
    }

    ActionsChainStepPtr & operator[](size_t index)
    {
        return steps[index];
    }

    const ActionsChainStepPtr & operator[](size_t index) const
    {
        return steps[index];
    }

    /// Get last step
    ActionsChainStep * getLastStep()
    {
        return steps.back().get();
    }

    /// Get last step or throw exception if chain is empty
    ActionsChainStep * getLastStepOrThrow()
    {
        if (steps.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsChain is empty");

        return steps.back().get();
    }

    /// Get last step index
    size_t getLastStepIndex()
    {
        return steps.size() - 1;
    }

    /// Get last step index or throw exception if chain is empty
    size_t getLastStepIndexOrThrow()
    {
        if (steps.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsChain is empty");

        return steps.size() - 1;
    }

    /// Get last step available output columns
    const ColumnsWithTypeAndName & getLastStepAvailableOutputColumns() const
    {
        return steps.back()->getAvailableOutputColumns();
    }

    /// Get last step available output columns or throw exception if chain is empty
    const ColumnsWithTypeAndName & getLastStepAvailableOutputColumnsOrThrow() const
    {
        if (steps.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsChain is empty");

        return steps.back()->getAvailableOutputColumns();
    }

    /// Get last step available output columns or null if chain is empty
    const ColumnsWithTypeAndName * getLastStepAvailableOutputColumnsOrNull() const
    {
        if (steps.empty())
            return nullptr;

        return &steps.back()->getAvailableOutputColumns();
    }

    /// Finalize chain
    void finalize();

    /// Dump chain into buffer
    void dump(WriteBuffer & buffer) const;

    /// Dump chain
    String dump() const;

private:
    ActionsChainSteps steps;
};

}
