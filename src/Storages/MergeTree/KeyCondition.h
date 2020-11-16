#pragma once

#include <sstream>
#include <optional>

#include <Interpreters/Set.h>
#include <Core/SortDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/FieldRange.h>


namespace DB
{

class Context;
class IFunction;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Condition on the index.
  *
  * Consists of the conditions for the key belonging to all possible ranges or sets,
  *  as well as logical operators AND/OR/NOT above these conditions.
  *
  * Constructs a reverse polish notation from these conditions
  *  and can calculate (interpret) its satisfiability over key ranges.
  */
class KeyCondition
{
public:
    /// Does not take into account the SAMPLE section. all_columns - the set of all columns of the table.
    KeyCondition(
        const SelectQueryInfo & query_info,
        const Context & context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false,
        bool strict_ = false);

    /// Whether the condition and its negation are feasible in the direct product of single column ranges specified by `hyperrectangle`.
    BoolMask checkInHyperrectangle(
        const std::vector<Range> & hyperrectangle,
        const DataTypes & data_types) const;

    /// Whether the condition and its negation are (independently) feasible in the key range.
    /// left_key and right_key must contain all fields in the sort_descr in the appropriate order.
    /// data_types - the types of the key columns.
    /// Argument initial_mask is used for early exiting the implementation when we do not care about
    /// one of the resulting mask components (see BoolMask::consider_only_can_be_XXX).
    BoolMask checkInRange(
        size_t used_key_size,
        const FieldRef * left_key,
        const FieldRef* right_key,
        const DataTypes & data_types,
        BoolMask initial_mask = BoolMask(false, false)) const;

    /// Are the condition and its negation valid in a semi-infinite (not limited to the right) key range.
    /// left_key must contain all the fields in the sort_descr in the appropriate order.
    BoolMask checkAfter(
        size_t used_key_size,
        const FieldRef * left_key,
        const DataTypes & data_types,
        BoolMask initial_mask = BoolMask(false, false)) const;

    /// Same as checkInRange, but calculate only may_be_true component of a result.
    /// This is more efficient than checkInRange(...).can_be_true.
    bool mayBeTrueInRange(
        size_t used_key_size,
        const FieldRef * left_key,
        const FieldRef * right_key,
        const DataTypes & data_types) const;

    /// Same as checkAfter, but calculate only may_be_true component of a result.
    /// This is more efficient than checkAfter(...).can_be_true.
    bool mayBeTrueAfter(
        size_t used_key_size,
        const FieldRef * left_key,
        const DataTypes & data_types) const;

    /// Checks that the index can not be used.
    bool alwaysUnknownOrTrue() const;

    /// Get the maximum number of the key element used in the condition.
    size_t getMaxKeyColumn() const;

    bool hasMonotonicFunctionsChain() const;

    /// Impose an additional condition: the value in the column `column` must be in the range `range`.
    /// Returns whether there is such a column in the key.
    bool addCondition(const String & column, const Range & range);

    String toString() const;


    /** A chain of possibly monotone functions.
      * If the key column is wrapped in functions that can be monotonous in some value ranges
      * (for example: -toFloat64(toDayOfWeek(date))), then here the functions will be located: toDayOfWeek, toFloat64, negate.
      */
    using FunctionsChain = std::vector<FunctionBasePtr>;
    using FunctionArgumentStack = std::vector<size_t>;

    /** Computes value of constant expression and its data type.
      * Returns false, if expression isn't constant.
      */
    static bool getConstant(
        const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type);

    static Block getBlockWithConstants(
        const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, const Context & context);

    bool matchesExactContinuousRange() const;

    static std::optional<Range> applyMonotonicFunctionsChainToRange(
        Range key_range,
        const FunctionsChain & functions,
        DataTypePtr current_type,
        bool single_point = false);

    static std::optional<RangeSet> applyMonotonicFunctionsChainToRangeSet(
        RangeSet key_range_set,
        const FunctionsChain & functions,
        DataTypePtr current_type);

    static std::optional<RangeSet> applyInvertibleFunctionsChainToRange(
        RangeSet key_range_set,
        const FunctionsChain & functions,
        const FunctionArgumentStack& argument_stack);

private:
    /// The expression is stored as Reverse Polish Notation.
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN_RANGE,
            FUNCTION_NOT_IN_RANGE,
            FUNCTION_IN_SET,
            FUNCTION_NOT_IN_SET,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement() {}
        RPNElement(Function function_) : function(function_) {}
        RPNElement(Function function_, size_t key_column_) : function(function_), key_column(key_column_) {}
        RPNElement(Function function_, size_t key_column_, const Range & range_)
            : function(function_), range(range_), key_column(key_column_) {}

        String toString() const;

        Function function = FUNCTION_UNKNOWN;

        /// For FUNCTION_IN_RANGE and FUNCTION_NOT_IN_RANGE.
        Range range;
        size_t key_column = 0;
        DataTypePtr data_type;
        FunctionArgumentStack function_argument_stack;
        /// For FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
        using MergeTreeSetIndexPtr = std::shared_ptr<const MergeTreeSetIndex>;
        MergeTreeSetIndexPtr set_index;

        mutable FunctionsChain monotonic_functions_chain;  /// Function execution does not violate its constancy.
        mutable FunctionsChain invertible_functions_chain;
    };

    using RPN = std::vector<RPNElement>;
    using ColumnIndices = std::map<String, size_t>;

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value)>;

public:
    static const AtomMap atom_map;

private:
    BoolMask checkInRange(
        size_t used_key_size,
        const FieldRef * left_key,
        const FieldRef * right_key,
        const DataTypes & data_types,
        bool right_bounded,
        BoolMask initial_mask) const;

    void traverseAST(const ASTPtr & node, const Context & context, Block & block_with_constants);
    bool tryParseAtomFromAST(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out);
    static bool tryParseLogicalOperatorFromAST(const ASTFunction * func, RPNElement & out);

    /** Whether the given node is a key column
      *  or an expression in which key column is wrapped in a chain of functions,
      *  that can be monotonic on certain ranges?
      * Returns the key column number, the type of the resulting expression
      *  and fills the chain of possibly-monotonic functions if those conditions are met.
      * Is the given node is an expression,
      * which can be deduced from one of the key expressions by applying invertible functions?
      * If found, returns the initial key column number, a chain of functions, which inverse needs to be applied,
      * as well as a stack of corresponding argument indices.
      * Those methods are combined:
      * If our query is negate(y) < 5, whilst the key is (z, zCurve(x, y)) we would get:
      * key column number = 1
      * monotonic functions chain: [negate]
      * invertible functions chain: [zCurve]
      * argument stack: [1] (arguments are indexed from 0)
      */
    bool isColumnPossiblyAnArgumentOfInvertibleFunctionsInKeyExpr(
        const String & name,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        FunctionsChain & out_invertible_functions_chain,
        FunctionArgumentStack & out_function_argument_stack);

    bool isColumnPossiblyAnArgumentOfInvertibleFunctionsInKeyExprImpl(
        const String & name,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        FunctionsChain & out_invertible_functions_chain,
        FunctionArgumentStack & out_function_argument_stack);

    bool isKeyPossiblyWrappedByMonotonicOrInvertibleFunctions(
        const ASTPtr & node,
        const Context & context,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        DataTypePtr & out_func_expr_type,
        FunctionsChain & out_monotonic_functions_chain,
        FunctionsChain & out_invertible_functions_chain,
        FunctionArgumentStack & out_function_argument_stack);

    bool isKeyPossiblyWrappedByMonotonicOrInvertibleFunctionsImpl(
        const ASTPtr & node,
        const Context & context,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        FunctionsChain & out_monotonic_functions_chain,
        DataTypePtr & current_type,
        FunctionsChain & out_invertible_functions_chain,
        FunctionArgumentStack & out_function_argument_stack);

    bool canConstantBeWrappedByMonotonicFunctions(
        const ASTPtr & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const ASTPtr & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    /// If it's possible to make an RPNElement
    /// that will filter values (possibly tuples) by the content of 'prepared_set',
    /// do it and return true.
    bool tryPrepareSetIndex(
        const ASTs & args,
        const Context & context,
        RPNElement & out,
        size_t & out_key_column_num);

    RPN rpn;

    ColumnIndices key_columns;
    ExpressionActionsPtr key_expr;
    PreparedSets prepared_sets;

    // If true, always allow key_expr to be wrapped by function
    bool single_point;
    // If true, do not use always_monotonic information to transform constants
    bool strict;
};

}
