#pragma once

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <variant>

#include "config_core.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Sequence of actions on the block.
/// Is used to calculate expressions.
///
/// Takes ActionsDAG and orders actions using top-sort.
class ExpressionActions
{
public:
    using Node = ActionsDAG::Node;

    struct Argument
    {
        /// Position in ExecutionContext::columns
        size_t pos = 0;
        /// True if there is another action which will use this column.
        /// Otherwise column will be removed.
        bool needed_later = false;
    };

    using Arguments = std::vector<Argument>;

    struct Action
    {
        const Node * node;
        Arguments arguments;
        size_t result_position;

        /// Determine if this action should be executed lazily. If it should and the node type is FUNCTION, then the function
        /// won't be executed and will be stored with it's arguments in ColumnFunction with isShortCircuitArgument() = true.
        bool is_lazy_executed;

        std::string toString() const;
        JSONBuilder::ItemPtr toTree() const;
    };

    using Actions = std::vector<Action>;

    /// This map helps to find input position by it's name.
    /// Key is a view to input::result_name.
    /// Result is a list because it is allowed for inputs to have same names.
    using NameToInputMap = std::unordered_map<std::string_view, std::list<size_t>>;

private:
    ActionsDAGPtr actions_dag;
    Actions actions;
    size_t num_columns = 0;

    NamesAndTypesList required_columns;
    NameToInputMap input_positions;
    ColumnNumbers result_positions;
    Block sample_block;

    ExpressionActionsSettings settings;

public:
    ExpressionActions() = delete;
    explicit ExpressionActions(ActionsDAGPtr actions_dag_, const ExpressionActionsSettings & settings_ = {});
    ExpressionActions(const ExpressionActions &) = default;
    ExpressionActions & operator=(const ExpressionActions &) = default;

    const Actions & getActions() const { return actions; }
    const std::list<Node> & getNodes() const { return actions_dag->getNodes(); }
    const ActionsDAG & getActionsDAG() const { return *actions_dag; }
    const ColumnNumbers & getResultPositions() const { return result_positions; }
    const ExpressionActionsSettings & getSettings() const { return settings; }

    /// Get a list of input columns.
    Names getRequiredColumns() const;
    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return required_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block, size_t & num_rows, bool dry_run = false) const;
    /// The same, but without `num_rows`. If result block is empty, adds `_dummy` column to keep block size.
    void execute(Block & block, bool dry_run = false) const;

    bool hasArrayJoin() const;
    void assertDeterministic() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;
    JSONBuilder::ItemPtr toTree() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    /// Check if column is always zero. True if it's definite, false if we can't say for sure.
    /// Call it only after subqueries for sets were executed.
    bool checkColumnIsAlwaysFalse(const String & column_name) const;

    ExpressionActionsPtr clone() const;

private:
    void checkLimits(const ColumnsWithTypeAndName & columns) const;

    void linearizeActions(const std::unordered_set<const Node *> & lazy_executed_nodes);
};


/** The sequence of transformations over the block.
  * It is assumed that the result of each step is fed to the input of the next step.
  * Used to execute parts of the query individually.
  *
  * For example, you can create a chain of two steps:
  *     1) evaluate the expression in the WHERE clause,
  *     2) calculate the expression in the SELECT section,
  * and between the two steps do the filtering by value in the WHERE clause.
  */
struct ExpressionActionsChain : WithContext
{
    explicit ExpressionActionsChain(ContextPtr context_) : WithContext(context_) {}


    struct Step
    {
        virtual ~Step() = default;
        explicit Step(Names required_output_)
        {
            for (const auto & name : required_output_)
                required_output[name] = true;
        }

        /// Columns were added to the block before current step in addition to prev step output.
        NameSet additional_input;
        /// Columns which are required in the result of current step.
        /// Flag is true if column from required_output is needed only for current step and not used in next actions
        /// (and can be removed from block). Example: filter column for where actions.
        /// If not empty, has the same size with required_output; is filled in finalize().
        std::unordered_map<std::string, bool> required_output;

        void addRequiredOutput(const std::string & name) { required_output[name] = true; }

        virtual NamesAndTypesList getRequiredColumns() const = 0;
        virtual ColumnsWithTypeAndName getResultColumns() const = 0;
        /// Remove unused result and update required columns
        virtual void finalize(const NameSet & required_output_) = 0;
        /// Add projections to expression
        virtual void prependProjectInput() const = 0;
        virtual std::string dump() const = 0;

        /// Only for ExpressionActionsStep
        ActionsDAGPtr & actions();
        const ActionsDAGPtr & actions() const;
    };

    struct ExpressionActionsStep : public Step
    {
        ActionsDAGPtr actions_dag;

        explicit ExpressionActionsStep(ActionsDAGPtr actions_dag_, Names required_output_ = Names())
            : Step(std::move(required_output_))
            , actions_dag(std::move(actions_dag_))
        {
        }

        NamesAndTypesList getRequiredColumns() const override
        {
            return actions_dag->getRequiredColumns();
        }

        ColumnsWithTypeAndName getResultColumns() const override
        {
            return actions_dag->getResultColumns();
        }

        void finalize(const NameSet & required_output_) override
        {
            if (!actions_dag->isOutputProjected())
                actions_dag->removeUnusedActions(required_output_);
        }

        void prependProjectInput() const override
        {
            actions_dag->projectInput();
        }

        std::string dump() const override
        {
            return actions_dag->dumpDAG();
        }
    };

    struct ArrayJoinStep : public Step
    {
        ArrayJoinActionPtr array_join;
        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_);

        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const NameSet & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before ARRAY JOIN ?
        std::string dump() const override { return "ARRAY JOIN"; }
    };

    struct JoinStep : public Step
    {
        std::shared_ptr<TableJoin> analyzed_join;
        JoinPtr join;

        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        JoinStep(std::shared_ptr<TableJoin> analyzed_join_, JoinPtr join_, const ColumnsWithTypeAndName & required_columns_);
        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const NameSet & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before JOIN ?
        std::string dump() const override { return "JOIN"; }
    };

    using StepPtr = std::unique_ptr<Step>;
    using Steps = std::vector<StepPtr>;

    Steps steps;

    void addStep(NameSet non_constant_inputs = {});

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ActionsDAGPtr getLastActions(bool allow_empty = false)  // -V1071
    {
        if (steps.empty())
        {
            if (allow_empty)
                return {};
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);
        }

        return typeid_cast<ExpressionActionsStep *>(steps.back().get())->actions_dag;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return *steps.back();
    }

    Step & lastStep(const NamesAndTypesList & columns)
    {
        if (steps.empty())
            steps.emplace_back(std::make_unique<ExpressionActionsStep>(std::make_shared<ActionsDAG>(columns)));
        return *steps.back();
    }

    std::string dumpChain() const;
};

}
