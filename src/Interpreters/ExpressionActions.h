#pragma once

#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActionsSettings.h>
#include <Functions/IFunction.h>

#include <functional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

using CheckCancelled = std::function<bool()>;

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
        /// The position of the action which produced this argument in `ExpressionActions::actions`.
        size_t actions_pos = 0;
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

        /// True if neither the function of this action nor any of its descendants can throw an exception on
        /// invalid input values. As a counterexample, `divide(1, 0)` throws, so lazy execution of such an
        /// action must never be turned off - it protects the rows which are filtered out by short circuit.
        bool is_no_except = false;

        /// Positions in `ExpressionActions::actions` of the actions which use the result of this action.
        std::vector<size_t> parents_actions_pos;

        std::string toString() const;
        JSONBuilder::ItemPtr toTree() const;
    };

    using Actions = std::vector<Action>;

    /// This map helps to find input position by its name.
    /// Key is a view to input::result_name.
    /// Result is a list because it is allowed for inputs to have same names.
    using NameToInputMap = std::unordered_map<std::string_view, std::list<size_t>>;

protected:
    ActionsDAG actions_dag;
    Actions actions;
    size_t num_columns = 0;

    NamesAndTypesList required_columns;
    NameToInputMap input_positions;
    ColumnNumbers result_positions;
    Block sample_block;

    bool project_inputs = false;

    ExpressionActionsSettings settings;

public:
    explicit ExpressionActions(ActionsDAG actions_dag_, const ExpressionActionsSettings & settings_ = {}, bool project_inputs_ = false);
    ExpressionActions(ExpressionActions &&) = default;
    ExpressionActions & operator=(ExpressionActions &&) = default;
    virtual ~ExpressionActions() = default;

    /// Create either a plain or an adaptive instance, depending on the settings.
    /// Note that an adaptive instance is stateful and not thread safe, so it must not be shared between
    /// pipeline streams: create one per stream.
    static ExpressionActionsPtr create(ActionsDAG actions_dag_, const ExpressionActionsSettings & settings_ = {}, bool project_inputs_ = false);

    const Actions & getActions() const { return actions; }
    const std::list<Node> & getNodes() const { return actions_dag.getNodes(); }
    const ActionsDAG & getActionsDAG() const { return actions_dag; }
    const ColumnNumbers & getResultPositions() const { return result_positions; }
    const ExpressionActionsSettings & getSettings() const { return settings; }

    /// Get a list of input columns.
    Names getRequiredColumns() const;
    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return required_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    ///
    /// @param allow_duplicates_in_input - actions are allowed to have
    /// duplicated input (that will refer into the block). This is needed for
    /// preliminary query filtering (filterBlockWithExpression()), because they just
    /// pass available virtual columns, which cannot be moved in case they are
    /// used multiple times.
    /// @param check_cancelled - optional callback to check for cancellation after each action.
    void execute(
        Block & block,
        size_t & num_rows,
        bool dry_run = false,
        bool allow_duplicates_in_input = false,
        CheckCancelled check_cancelled = nullptr) const;
    /// The same, but without `num_rows`. If result block is empty, adds `_dummy` column to keep block size.
    void
    execute(Block & block, bool dry_run = false, bool allow_duplicates_in_input = false, CheckCancelled check_cancelled = nullptr) const;

    /// Positional execution for callers whose input structure is fixed (e.g. ExpressionTransform).
    ///
    /// `getInputPositions(header)` precomputes, once, the mapping (required input slot -> position in
    /// `header`); the result is passed to `executeOnColumns` on every chunk. This avoids the per-chunk
    /// name lookups and the construction of a `Block` name index (`index_by_name`) on both the input and
    /// the output, which is significant when the header has very many columns.
    ///
    /// `executeOnColumns` consumes `columns` (chunk data, in `header` order) and returns the result
    /// columns in `getSampleBlock()` order. It assumes `allow_duplicates_in_input == false`.
    std::vector<ssize_t> getInputPositions(const Block & header) const;
    Columns executeOnColumns(
        Columns columns,
        const Block & header,
        const std::vector<ssize_t> & input_positions_for_header,
        size_t & num_rows,
        bool dry_run = false,
        CheckCancelled check_cancelled = nullptr) const;

    bool hasArrayJoin() const;
    void assertDeterministic() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    void describeActions(WriteBuffer & out, std::string_view prefix) const;

    JSONBuilder::ItemPtr toTree() const;

    /// Find the column with the smallest estimated in-memory size.
    /// When skip_subcolumns=true (default), meta-subcolumns like .size0/.keys
    /// are skipped — correct for storage column lists but not for subquery
    /// projections where all entries are valid query-level outputs.
    static NameAndTypePair getSmallestColumn(const NamesAndTypesList & columns, bool skip_subcolumns = true);

    ExpressionActionsPtr clone() const;

protected:
    ExpressionActions() = default;
    void checkLimits(const ColumnsWithTypeAndName & columns) const;

    void linearizeActions(const std::unordered_set<const Node *> & lazy_executed_nodes);

    /// This struct stores context needed to execute actions.
    ///
    /// Execution model is following:
    ///   * execution is performed over list of columns (with fixed size = ExpressionActions::num_columns)
    ///   * every argument has fixed position in columns list, every action has fixed position for result
    ///   * if argument is not needed anymore (Argument::needed_later == false), it is removed from list
    ///   * argument for INPUT is in inputs[inputs_pos[argument.pos]]
    ///
    /// Columns on positions `ExpressionActions::result_positions` are inserted back into block.
    struct ExecutionContext
    {
        ColumnsWithTypeAndName & inputs;
        ColumnsWithTypeAndName columns = {};
        std::vector<ssize_t> inputs_pos = {};
        size_t num_rows = 0;
    };

    void executeAction(size_t action_index, ExecutionContext & execution_context, bool dry_run, bool allow_duplicates_in_input) const;

    /// Hooks which allow a derived class to change how the functions are executed.

    /// Whether the action must be wrapped into a lazily executed ColumnFunction instead of being executed now.
    virtual bool shouldExecuteLazily(size_t action_index) const { return actions[action_index].is_lazy_executed; }

    /// Execute the function of the action on the prepared arguments.
    virtual ColumnPtr executeFunction(
        size_t action_index,
        ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t num_rows,
        bool dry_run) const;

    /// Called after all the actions were executed on a block. `input_num_rows` is the number of rows the
    /// block had *before* the execution: row-multiplying actions such as `ARRAY JOIN` change the block size,
    /// and the profiling below has to be attributed to the rows the actions actually ran on.
    virtual void finalizeBlockExecution(size_t /*input_num_rows*/) const {}
};

/// AdaptiveExpressionActions builds upon ExpressionActions to enable dynamic evaluation of whether a
/// short-circuit function's argument should be lazily executed.
///
/// Lazy execution is not free: the rows which are not needed are filtered out and the result is expanded
/// back to the original size. When the filtered out part is small, or the argument itself is cheap, this
/// overhead is larger than the saved work. The execution of every function is profiled, and every
/// `update_on_every_rows` rows the decision is revisited: an action stops being executed lazily when
/// `estimated cost of the full execution < the cost of the lazy execution`, and it is enabled back when
/// its per-row cost grows significantly.
///
/// AdaptiveExpressionActions is stateful and not thread safe: create one instance per pipeline stream.
class AdaptiveExpressionActions : public ExpressionActions
{
public:
    explicit AdaptiveExpressionActions(
        ActionsDAG actions_dag_, const ExpressionActionsSettings & settings_ = {}, bool project_inputs_ = false);

protected:
    bool shouldExecuteLazily(size_t action_index) const override;

    ColumnPtr executeFunction(
        size_t action_index,
        ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t num_rows,
        bool dry_run) const override;

    void finalizeBlockExecution(size_t input_num_rows) const override;

private:
    struct ActionState
    {
        /// Indicates whether it is beneficial to execute this action lazily with short-circuit evaluation.
        /// If the cost of filtering out unnecessary rows and expanding back to a full column is greater
        /// than the cost of fully executing the column, then it is not worth executing lazily.
        bool is_lazy_execution_efficient = false;
        /// Whether the action is a short-circuit function (`and`, `or`, `if`, `multiIf`). Such an action is
        /// never lazily executed itself, but it is a transparent wrapper for the cost of its arguments.
        /// Unlike the field above, this one is computed once and never changes.
        bool is_short_circuit_function = false;
        /// The execution profile of the current round. `is_lazy_execution_efficient` is updated after each round.
        FunctionExecutionProfile current_round_profile;
        /// The execution profile of all the rounds.
        FunctionExecutionProfile total_profile;
    };

    /// The state is mutable because the execution methods are const: an instance is never shared between
    /// threads (see the note above), so no synchronization is needed.
    mutable std::vector<ActionState> action_states;
    /// The number of input rows (before any row-multiplying action) processed in the current round.
    mutable size_t current_round_input_rows = 0;

    void accumulateProfile(size_t action_index, const FunctionExecutionProfile & profile) const;
    size_t getActionInputRows(size_t action_index) const;
    void updateActionsParentsProfile() const;
    void updateActionParentProfile(size_t action_index, size_t extra_elapsed) const;
    void identifyNonBeneficialLazyActions() const;
};

namespace ExpressionActionsChainSteps
{

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
    virtual void prependProjectInput() = 0;
    virtual std::string dump() const = 0;

    /// Only for ExpressionActionsStep
    ActionsAndProjectInputsFlagPtr & actions();
    const ActionsAndProjectInputsFlagPtr & actions() const;
};

struct ExpressionActionsStep : public Step
{
    ActionsAndProjectInputsFlagPtr actions_and_flags;
    bool is_final_projection = false;

    explicit ExpressionActionsStep(ActionsAndProjectInputsFlagPtr actiactions_and_flags_, Names required_output_ = Names())
        : Step(std::move(required_output_))
        , actions_and_flags(std::move(actiactions_and_flags_))
    {
    }

    NamesAndTypesList getRequiredColumns() const override
    {
        return actions_and_flags->dag.getRequiredColumns();
    }

    ColumnsWithTypeAndName getResultColumns() const override
    {
        return actions_and_flags->dag.getResultColumns();
    }

    void finalize(const NameSet & required_output_) override
    {
        if (!is_final_projection)
            actions_and_flags->dag.removeUnusedActions(required_output_);
    }

    void prependProjectInput() override
    {
        actions_and_flags->project_input = true;
    }

    std::string dump() const override
    {
        return actions_and_flags->dag.dumpDAG();
    }
};

struct ArrayJoinStep : public Step
{
    const NameSet array_join_columns;
    NamesAndTypesList required_columns;
    ColumnsWithTypeAndName result_columns;

    ArrayJoinStep(const Names & array_join_columns_, ColumnsWithTypeAndName required_columns_);

    NamesAndTypesList getRequiredColumns() const override { return required_columns; }
    ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
    void finalize(const NameSet & required_output_) override;
    void prependProjectInput() override {} /// TODO: remove unused columns before ARRAY JOIN ?
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
    void prependProjectInput() override {} /// TODO: remove unused columns before JOIN ?
    std::string dump() const override { return "JOIN"; }
};

}

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

    using StepPtr = std::unique_ptr<ExpressionActionsChainSteps::Step>;
    using Steps = std::vector<StepPtr>;

    Steps steps;

    void addStep(NameSet non_constant_inputs = {});

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ExpressionActionsChainSteps::ExpressionActionsStep * getLastExpressionStep(bool allow_empty = false);

    ActionsAndProjectInputsFlagPtr getLastActions(bool allow_empty = false)
    {
        if (auto * step = getLastExpressionStep(allow_empty))
            return step->actions_and_flags;

        return nullptr;
    }

    ExpressionActionsChainSteps::Step & getLastStep()
    {
        if (steps.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty ExpressionActionsChain");

        return *steps.back();
    }

    ExpressionActionsChainSteps::Step & lastStep(const NamesAndTypesList & columns)
    {
        if (steps.empty())
            return addStep(columns);
        return *steps.back();
    }

    ExpressionActionsChainSteps::Step & addStep(const NamesAndTypesList & columns)
    {
        return *steps.emplace_back(std::make_unique<ExpressionActionsChainSteps::ExpressionActionsStep>(
            std::make_shared<ActionsAndProjectInputsFlag>(ActionsDAG(columns), false)));
    }

    std::string dumpChain() const;
};

}
