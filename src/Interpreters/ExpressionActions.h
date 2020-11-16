#pragma once

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>

#include <variant>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


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
    using Index = ActionsDAG::Index;

    struct Argument
    {
        /// Position in ExecutionContext::columns
        size_t pos;
        /// True if there is another action which will use this column.
        /// Otherwise column will be removed.
        bool needed_later;
    };

    using Arguments = std::vector<Argument>;

    struct Action
    {
        const Node * node;
        Arguments arguments;
        size_t result_position;

        std::string toString() const;
    };

    using Actions = std::vector<Action>;

private:

    ActionsDAGPtr actions_dag;
    Actions actions;
    size_t num_columns = 0;

    NamesAndTypesList required_columns;
    ColumnNumbers result_positions;
    Block sample_block;

public:
    ~ExpressionActions();
    explicit ExpressionActions(ActionsDAGPtr actions_dag_);
    ExpressionActions(const ExpressionActions &) = default;
    ExpressionActions & operator=(const ExpressionActions &) = default;

    const Actions & getActions() const { return actions; }
    const std::list<Node> & getNodes() const { return actions_dag->getNodes(); }
    const ActionsDAG & getActionsDAG() const { return *actions_dag; }

    /// Get a list of input columns.
    Names getRequiredColumns() const;
    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return required_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block, size_t & num_rows, bool dry_run = false) const;
    /// The same, but without `num_rows`. If result block is empty, adds `_dummy` column to keep block size.
    void execute(Block & block, bool dry_run = false) const;

    bool hasArrayJoin() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    /// Check if column is always zero. True if it's definite, false if we can't say for sure.
    /// Call it only after subqueries for sets were executed.
    bool checkColumnIsAlwaysFalse(const String & column_name) const;

    ExpressionActionsPtr clone() const;

private:
    ExpressionActions() = default;

    void checkLimits(const ColumnsWithTypeAndName & columns) const;

    void linearizeActions();
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
struct ExpressionActionsChain
{
    explicit ExpressionActionsChain(const Context & context_) : context(context_) {}


    struct Step
    {
        virtual ~Step() = default;
        explicit Step(Names required_output_) : required_output(std::move(required_output_)) {}

        /// Columns were added to the block before current step in addition to prev step output.
        NameSet additional_input;
        /// Columns which are required in the result of current step.
        Names required_output;
        /// True if column from required_output is needed only for current step and not used in next actions
        /// (and can be removed from block). Example: filter column for where actions.
        /// If not empty, has the same size with required_output; is filled in finalize().
        std::vector<bool> can_remove_required_output;

        virtual NamesAndTypesList getRequiredColumns() const = 0;
        virtual ColumnsWithTypeAndName getResultColumns() const = 0;
        /// Remove unused result and update required columns
        virtual void finalize(const Names & required_output_) = 0;
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

        void finalize(const Names & required_output_) override
        {
            if (!actions_dag->getSettings().projected_output)
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
        void finalize(const Names & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before ARRAY JOIN ?
        std::string dump() const override { return "ARRAY JOIN"; }
    };

    struct JoinStep : public Step
    {
        std::shared_ptr<TableJoin> analyzed_join;
        JoinPtr join;

        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        JoinStep(std::shared_ptr<TableJoin> analyzed_join_, JoinPtr join_, ColumnsWithTypeAndName required_columns_);
        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const Names & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before JOIN ?
        std::string dump() const override { return "JOIN"; }
    };

    using StepPtr = std::unique_ptr<Step>;
    using Steps = std::vector<StepPtr>;

    const Context & context;
    Steps steps;

    void addStep(NameSet non_constant_inputs = {});

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ActionsDAGPtr getLastActions(bool allow_empty = false)
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
