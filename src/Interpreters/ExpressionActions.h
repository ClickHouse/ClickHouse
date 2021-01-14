#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <unordered_map>
#include <unordered_set>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <DataTypes/DataTypeArray.h>

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

class Context;
class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class ExpressionActions;
class CompiledExpressionCache;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

/** Action on the block.
  */
struct ExpressionAction
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
public:
    enum Type
    {
        ADD_COLUMN,
        REMOVE_COLUMN,
        COPY_COLUMN,

        APPLY_FUNCTION,

        /// Replaces the source column with array into column with elements.
        /// Duplicates the values in the remaining columns by the number of elements in the arrays.
        /// Source column is removed from block.
        ARRAY_JOIN,

        /// Reorder and rename the columns, delete the extra ones. The same column names are allowed in the result.
        PROJECT,
        /// Add columns with alias names. This columns are the same as non-aliased. PROJECT columns if you need to modify them.
        ADD_ALIASES,
    };

    Type type{};

    /// For ADD/REMOVE/ARRAY_JOIN/COPY_COLUMN.
    std::string source_name;
    std::string result_name;
    DataTypePtr result_type;

    /// If COPY_COLUMN can replace the result column.
    bool can_replace = false;

    /// For ADD_COLUMN.
    ColumnPtr added_column;

    /// For APPLY_FUNCTION.
    /// OverloadResolver is used before action was added to ExpressionActions (when we don't know types of arguments).
    FunctionOverloadResolverPtr function_builder;

    /// Can be used after action was added to ExpressionActions if we want to get function signature or properties like monotonicity.
    FunctionBasePtr function_base;
    /// Prepared function which is used in function execution.
    ExecutableFunctionPtr function;
    Names argument_names;
    bool is_function_compiled = false;

    /// For JOIN
    std::shared_ptr<const TableJoin> table_join;
    JoinPtr join;

    /// For PROJECT.
    NamesWithAliases projection;

    /// If result_name_ == "", as name "function_name(arguments separated by commas) is used".
    static ExpressionAction applyFunction(
            const FunctionOverloadResolverPtr & function_, const std::vector<std::string> & argument_names_, std::string result_name_ = "");

    static ExpressionAction addColumn(const ColumnWithTypeAndName & added_column_);
    static ExpressionAction removeColumn(const std::string & removed_name);
    static ExpressionAction copyColumn(const std::string & from_name, const std::string & to_name, bool can_replace = false);
    static ExpressionAction project(const NamesWithAliases & projected_columns_);
    static ExpressionAction project(const Names & projected_columns_);
    static ExpressionAction addAliases(const NamesWithAliases & aliased_columns_);
    static ExpressionAction arrayJoin(std::string source_name, std::string result_name);

    /// Which columns necessary to perform this action.
    Names getNeededColumns() const;

    std::string toString() const;

    bool operator==(const ExpressionAction & other) const;

    struct ActionHash
    {
        UInt128 operator()(const ExpressionAction & action) const;
    };

private:
    friend class ExpressionActions;

    void prepare(Block & sample_block, const Settings & settings, NameSet & names_not_for_constant_folding);
    void execute(Block & block, bool dry_run) const;
};

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG
{
public:

    enum class Type
    {
        /// Column which must be in input.
        INPUT,
        /// Constant column with known value.
        COLUMN,
        /// Another one name for column.
        ALIAS,
        /// Function arrayJoin. Specially separated because it changes the number of rows.
        ARRAY_JOIN,
        FUNCTION,
    };

    struct Node
    {
        std::vector<Node *> children;
        /// This field is filled if current node is replaced by existing node with the same name.
        Node * renaming_parent = nullptr;

        Type type;

        std::string result_name;
        DataTypePtr result_type;

        std::string unique_column_name_for_array_join;

        FunctionOverloadResolverPtr function_builder;
        /// Can be used after action was added to ExpressionActions if we want to get function signature or properties like monotonicity.
        FunctionBasePtr function_base;
        /// Prepared function which is used in function execution.
        ExecutableFunctionPtr function;

        /// For COLUMN node and propagated constants.
        ColumnPtr column;
        /// Some functions like `ignore()` always return constant but can't be replaced by constant it.
        /// We calculate such constants in order to avoid unnecessary materialization, but prohibit it's folding.
        bool allow_constant_folding = true;
    };

    using Index = std::unordered_map<std::string_view, Node *>;

private:
    std::list<Node> nodes;
    Index index;

public:
    ActionsDAG() = default;
    ActionsDAG(const ActionsDAG &) = delete;
    ActionsDAG & operator=(const ActionsDAG &) = delete;
    ActionsDAG(const NamesAndTypesList & inputs);
    ActionsDAG(const ColumnsWithTypeAndName & inputs);

    const Index & getIndex() const { return index; }

    ColumnsWithTypeAndName getResultColumns() const;
    NamesAndTypesList getNamesAndTypesList() const;
    Names getNames() const;
    std::string dumpNames() const;

    const Node & addInput(std::string name, DataTypePtr type);
    const Node & addInput(ColumnWithTypeAndName column);
    const Node & addColumn(ColumnWithTypeAndName column);
    const Node & addAlias(const std::string & name, std::string alias, bool can_replace = false);
    const Node & addArrayJoin(const std::string & source_name, std::string result_name, std::string unique_column_name);
    const Node & addFunction(
            const FunctionOverloadResolverPtr & function,
            const Names & argument_names,
            std::string result_name,
            bool compile_expressions);

    ExpressionActionsPtr buildExpressions(const Context & context);

private:
    Node & addNode(Node node, bool can_replace = false);
    Node & getNode(const std::string & name);
};

using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/** Contains a sequence of actions on the block.
  */
class ExpressionActions
{
public:
    using Actions = std::vector<ExpressionAction>;

    ExpressionActions(const NamesAndTypesList & input_columns_, const Context & context_);

    /// For constant columns the columns themselves can be contained in `input_columns_`.
    ExpressionActions(const ColumnsWithTypeAndName & input_columns_, const Context & context_);

    ~ExpressionActions();

    ExpressionActions(const ExpressionActions & other) = default;

    /// Add the input column.
    /// The name of the column must not match the names of the intermediate columns that occur when evaluating the expression.
    /// The expression must not have any PROJECT actions.
    void addInput(const ColumnWithTypeAndName & column);
    void addInput(const NameAndTypePair & column);

    void add(const ExpressionAction & action);

    /// Adds new column names to out_new_columns (formed as a result of the added action).
    void add(const ExpressionAction & action, Names & out_new_columns);

    /// Adds to the beginning the removal of all extra columns.
    void prependProjectInput();

    /// Splits actions into two parts. Returned half may be swapped with ARRAY JOIN.
    /// Returns nullptr if no actions may be moved before ARRAY JOIN.
    ExpressionActionsPtr splitActionsBeforeArrayJoin(const NameSet & array_joined_columns);

    /// - Adds actions to delete all but the specified columns.
    /// - Removes unused input columns.
    /// - Can somehow optimize the expression.
    /// - Does not reorder the columns.
    /// - Does not remove "unexpected" columns (for example, added by functions).
    /// - If output_columns is empty, leaves one arbitrary column (so that the number of rows in the block is not lost).
    void finalize(const Names & output_columns);

    const Actions & getActions() const { return actions; }

    /// Get a list of input columns.
    Names getRequiredColumns() const
    {
        Names names;
        for (const auto & input : input_columns)
            names.push_back(input.name);
        return names;
    }

    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return input_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block, bool dry_run = false) const;

    bool hasArrayJoin() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    const Settings & getSettings() const { return settings; }

    /// Check if column is always zero. True if it's definite, false if we can't say for sure.
    /// Call it only after subqueries for sets were executed.
    bool checkColumnIsAlwaysFalse(const String & column_name) const;

    struct ActionsHash
    {
        UInt128 operator()(const ExpressionActions::Actions & elems) const
        {
            SipHash hash;
            for (const ExpressionAction & act : elems)
                hash.update(ExpressionAction::ActionHash{}(act));
            UInt128 result;
            hash.get128(result.low, result.high);
            return result;
        }
    };

private:
    /// These columns have to be in input blocks (arguments of execute* methods)
    NamesAndTypesList input_columns;
    /// These actions will be executed on input blocks
    Actions actions;
    /// The example of result (output) block.
    Block sample_block;
    /// Columns which can't be used for constant folding.
    NameSet names_not_for_constant_folding;

    Settings settings;
#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledExpressionCache> compilation_cache;
#endif

    void checkLimits(Block & block) const;

    void addImpl(ExpressionAction action, Names & new_names);

    /// Move all arrayJoin as close as possible to the end.
    void optimizeArrayJoin();
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

        virtual const NamesAndTypesList & getRequiredColumns() const = 0;
        virtual const ColumnsWithTypeAndName & getResultColumns() const = 0;
        /// Remove unused result and update required columns
        virtual void finalize(const Names & required_output_) = 0;
        /// Add projections to expression
        virtual void prependProjectInput() const = 0;
        virtual std::string dump() const = 0;

        /// Only for ExpressionActionsStep
        ActionsDAGPtr & actions();
        const ActionsDAGPtr & actions() const;
        ExpressionActionsPtr getExpression() const;
    };

    struct ExpressionActionsStep : public Step
    {
        ActionsDAGPtr actions_dag;
        ExpressionActionsPtr actions;

        explicit ExpressionActionsStep(ActionsDAGPtr actions_, Names required_output_ = Names())
            : Step(std::move(required_output_))
            , actions_dag(std::move(actions_))
        {
        }

        const NamesAndTypesList & getRequiredColumns() const override
        {
            return actions->getRequiredColumnsWithTypes();
        }

        const ColumnsWithTypeAndName & getResultColumns() const override
        {
            return actions->getSampleBlock().getColumnsWithTypeAndName();
        }

        void finalize(const Names & required_output_) override
        {
            actions->finalize(required_output_);
        }

        void prependProjectInput() const override
        {
            actions->prependProjectInput();
        }

        std::string dump() const override
        {
            return actions->dumpActions();
        }
    };

    struct ArrayJoinStep : public Step
    {
        ArrayJoinActionPtr array_join;
        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_);

        const NamesAndTypesList & getRequiredColumns() const override { return required_columns; }
        const ColumnsWithTypeAndName & getResultColumns() const override { return result_columns; }
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
        const NamesAndTypesList & getRequiredColumns() const override { return required_columns; }
        const ColumnsWithTypeAndName & getResultColumns() const override { return result_columns; }
        void finalize(const Names & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before JOIN ?
        std::string dump() const override { return "JOIN"; }
    };

    using StepPtr = std::unique_ptr<Step>;
    using Steps = std::vector<StepPtr>;

    const Context & context;
    Steps steps;

    void addStep();

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ExpressionActionsPtr getLastActions(bool allow_empty = false)
    {
        if (steps.empty())
        {
            if (allow_empty)
                return {};
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);
        }

        auto * step = typeid_cast<ExpressionActionsStep *>(steps.back().get());
        step->actions = step->actions_dag->buildExpressions(context);
        return step->actions;
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
