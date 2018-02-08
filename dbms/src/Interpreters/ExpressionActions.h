#pragma once

#include <Interpreters/Settings.h>
#include <Core/Names.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>

#include <unordered_set>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;

class Join;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class IFunctionBuilder;
using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;


/** Action on the block.
  */
struct ExpressionAction
{
public:
    enum Type
    {
        ADD_COLUMN,
        REMOVE_COLUMN,
        COPY_COLUMN,

        APPLY_FUNCTION,

        /** Replaces the specified columns with arrays into columns with elements.
            * Duplicates the values in the remaining columns by the number of elements in the arrays.
            * Arrays must be parallel (have the same lengths).
            */
        ARRAY_JOIN,

        JOIN,

        /// Reorder and rename the columns, delete the extra ones. The same column names are allowed in the result.
        PROJECT,
    };

    Type type;

    /// For ADD/REMOVE/COPY_COLUMN.
    std::string source_name;
    std::string result_name;
    DataTypePtr result_type;

    /// For ADD_COLUMN.
    ColumnPtr added_column;

    /// For APPLY_FUNCTION and LEFT ARRAY JOIN.
    FunctionBuilderPtr function_builder;
    FunctionBasePtr function;
    Names argument_names;

    /// For ARRAY_JOIN
    NameSet array_joined_columns;
    bool array_join_is_left = false;

    /// For JOIN
    std::shared_ptr<const Join> join;
    NamesAndTypesList columns_added_by_join;

    /// For PROJECT.
    NamesWithAliases projection;

    /// If result_name_ == "", as name "function_name(arguments separated by commas) is used".
    static ExpressionAction applyFunction(
        const FunctionBuilderPtr & function_, const std::vector<std::string> & argument_names_, std::string result_name_ = "");

    static ExpressionAction addColumn(const ColumnWithTypeAndName & added_column_);
    static ExpressionAction removeColumn(const std::string & removed_name);
    static ExpressionAction copyColumn(const std::string & from_name, const std::string & to_name);
    static ExpressionAction project(const NamesWithAliases & projected_columns_);
    static ExpressionAction project(const Names & projected_columns_);
    static ExpressionAction arrayJoin(const NameSet & array_joined_columns, bool array_join_is_left, const Context & context);
    static ExpressionAction ordinaryJoin(std::shared_ptr<const Join> join_, const NamesAndTypesList & columns_added_by_join_);

    /// Which columns necessary to perform this action.
    /// If this `Action` is not already added to `ExpressionActions`, the returned list may be incomplete, because `prerequisites` are not taken into account.
    Names getNeededColumns() const;

    std::string toString() const;

private:
    friend class ExpressionActions;

    std::vector<ExpressionAction> getPrerequisites(Block & sample_block);
    void prepare(Block & sample_block);
    void execute(Block & block) const;
    void executeOnTotals(Block & block) const;
};


/** Contains a sequence of actions on the block.
  */
class ExpressionActions
{
public:
    using Actions = std::vector<ExpressionAction>;

    ExpressionActions(const NamesAndTypesList & input_columns_, const Settings & settings_)
        : input_columns(input_columns_), settings(settings_)
    {
        for (const auto & input_elem : input_columns)
            sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));
    }

    /// For constant columns the columns themselves can be contained in `input_columns_`.
    ExpressionActions(const ColumnsWithTypeAndName & input_columns_, const Settings & settings_)
        : settings(settings_)
    {
        for (const auto & input_elem : input_columns_)
        {
            input_columns.emplace_back(input_elem.name, input_elem.type);
            sample_block.insert(input_elem);
        }
    }

    /// Add the input column.
    /// The name of the column must not match the names of the intermediate columns that occur when evaluating the expression.
    /// The expression must not have any PROJECT actions.
    void addInput(const ColumnWithTypeAndName & column);
    void addInput(const NameAndTypePair & column);

    void add(const ExpressionAction & action);

    /// Adds new column names to out_new_columns
    ///  (formed as a result of the added action and its prerequisites).
    void add(const ExpressionAction & action, Names & out_new_columns);

    /// Adds to the beginning the removal of all extra columns.
    void prependProjectInput();

    /// Add the specified ARRAY JOIN action to the beginning. Change the appropriate input types to arrays.
    /// If there are unknown columns in the ARRAY JOIN list, take their types from sample_block, and immediately after ARRAY JOIN remove them.
    void prependArrayJoin(const ExpressionAction & action, const Block & sample_block);

    /// If the last action is ARRAY JOIN, and it does not affect the columns from required_columns, discard and return it.
    /// Change the corresponding output types to arrays.
    bool popUnusedArrayJoin(const Names & required_columns, ExpressionAction & out_action);

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
        for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
            names.push_back(it->name);
        return names;
    }

    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return input_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block) const;

    /** Execute the expression on the block of total values.
      * Almost the same as `execute`. The difference is only when JOIN is executed.
      */
    void executeOnTotals(Block & block) const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string getID() const;

    std::string dumpActions() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    BlockInputStreamPtr createStreamWithNonJoinedDataIfFullOrRightJoin(size_t max_block_size) const;

private:
    NamesAndTypesList input_columns;
    Actions actions;
    Block sample_block;
    Settings settings;

    void checkLimits(Block & block) const;

    /// Adds all `prerequisites` first, then the action itself.
    /// current_names - columns whose `prerequisites` are currently being processed.
    void addImpl(ExpressionAction action, NameSet & current_names, Names & new_names);

    /// Try to improve something without changing the lists of input and output columns.
    void optimize();
    /// Move all arrayJoin as close as possible to the end.
    void optimizeArrayJoin();
};

using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;


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
    struct Step
    {
        ExpressionActionsPtr actions;
        Names required_output;

        Step(const ExpressionActionsPtr & actions_ = nullptr, const Names & required_output_ = Names())
            : actions(actions_), required_output(required_output_) {}
    };

    using Steps = std::vector<Step>;

    Settings settings;
    Steps steps;

    void addStep();

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ExpressionActionsPtr getLastActions()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back().actions;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back();
    }

    std::string dumpChain();
};

}
