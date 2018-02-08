#pragma once

#include <sstream>
#include <optional>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>
#include <Core/SortDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

class IFunction;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/** Range with open or closed ends; possibly unbounded.
  */
struct Range
{
private:
    static bool equals(const Field & lhs, const Field & rhs);
    static bool less(const Field & lhs, const Field & rhs);

public:
    Field left;                       /// the left border, if any
    Field right;                      /// the right border, if any
    bool left_bounded = false;        /// bounded at the left
    bool right_bounded = false;       /// bounded at the right
    bool left_included = false;       /// includes the left border, if any
    bool right_included = false;      /// includes the right border, if any

    /// The whole unversum.
    Range() {}

    /// One point.
    Range(const Field & point)
        : left(point), right(point), left_bounded(true), right_bounded(true), left_included(true), right_included(true) {}

    /// A bounded two-sided range.
    Range(const Field & left_, bool left_included_, const Field & right_, bool right_included_)
        : left(left_), right(right_),
        left_bounded(true), right_bounded(true),
        left_included(left_included_), right_included(right_included_)
    {
        shrinkToIncludedIfPossible();
    }

    static Range createRightBounded(const Field & right_point, bool right_included)
    {
        Range r;
        r.right = right_point;
        r.right_bounded = true;
        r.right_included = right_included;
        r.shrinkToIncludedIfPossible();
        return r;
    }

    static Range createLeftBounded(const Field & left_point, bool left_included)
    {
        Range r;
        r.left = left_point;
        r.left_bounded = true;
        r.left_included = left_included;
        r.shrinkToIncludedIfPossible();
        return r;
    }

    /** Optimize the range. If it has an open boundary and the Field type is "loose"
      * - then convert it to closed, narrowing by one.
      * That is, for example, turn (0,2) into [1].
      */
    void shrinkToIncludedIfPossible()
    {
        if (left_bounded && !left_included)
        {
            if (left.getType() == Field::Types::UInt64 && left.get<UInt64>() != std::numeric_limits<UInt64>::max())
            {
                ++left.get<UInt64 &>();
                left_included = true;
            }
            if (left.getType() == Field::Types::Int64 && left.get<Int64>() != std::numeric_limits<Int64>::max())
            {
                ++left.get<Int64 &>();
                left_included = true;
            }
        }
        if (right_bounded && !right_included)
        {
            if (right.getType() == Field::Types::UInt64 && right.get<UInt64>() != std::numeric_limits<UInt64>::min())
            {
                --right.get<UInt64 &>();
                right_included = true;
            }
            if (right.getType() == Field::Types::Int64 && right.get<Int64>() != std::numeric_limits<Int64>::min())
            {
                --right.get<Int64 &>();
                right_included = true;
            }
        }
    }

    bool empty() const
    {
        return left_bounded && right_bounded
            && (less(right, left)
                || ((!left_included || !right_included) && !less(left, right)));
    }

    /// x contained in the range
    bool contains(const Field & x) const
    {
        return !leftThan(x) && !rightThan(x);
    }

    /// x is to the left
    bool rightThan(const Field & x) const
    {
        return (left_bounded
            ? !(less(left, x) || (left_included && equals(x, left)))
            : false);
    }

    /// x is to the right
    bool leftThan(const Field & x) const
    {
        return (right_bounded
            ? !(less(x, right) || (right_included && equals(x, right)))
            : false);
    }

    bool intersectsRange(const Range & r) const
    {
        /// r to the left of me.
        if (r.right_bounded
            && left_bounded
            && (less(r.right, left)
                || ((!left_included || !r.right_included)
                    && equals(r.right, left))))
            return false;

        /// r to the right of me.
        if (r.left_bounded
            && right_bounded
            && (less(right, r.left)                          /// ...} {...
                || ((!right_included || !r.left_included)    /// ...) [... or ...] (...
                    && equals(r.left, right))))
            return false;

        return true;
    }

    bool containsRange(const Range & r) const
    {
        /// r starts to the left of me.
        if (left_bounded
            && (!r.left_bounded
                || less(r.left, left)
                || (r.left_included
                    && !left_included
                    && equals(r.left, left))))
            return false;

        /// r ends right of me.
        if (right_bounded
            && (!r.right_bounded
                || less(right, r.right)
                || (r.right_included
                    && !right_included
                    && equals(r.right, right))))
            return false;

        return true;
    }

    void swapLeftAndRight()
    {
        std::swap(left, right);
        std::swap(left_bounded, right_bounded);
        std::swap(left_included, right_included);
    }

    String toString() const;
};

/// Class that extends arbitrary objects with infinities, like +-inf for floats
class FieldWithInfinity
{
public:
    enum Type
    {
        MINUS_INFINITY = -1,
        NORMAL = 0,
        PLUS_INFINITY = 1
    };

    explicit FieldWithInfinity(const Field & field_);
    FieldWithInfinity(Field && field_);

    static FieldWithInfinity getMinusInfinity();
    static FieldWithInfinity getPlusinfinity();

    bool operator<(const FieldWithInfinity & other) const;
    bool operator==(const FieldWithInfinity & other) const;

private:
    Field field;
    Type type;

    FieldWithInfinity(const Type type_);
};

/** Condition on the index.
  *
  * Consists of the conditions for the key belonging to all possible ranges or sets,
  *  as well as logical operators AND/OR/NOT above these conditions.
  *
  * Constructs a reverse polish notation from these conditions
  *  and can calculate (interpret) its satisfiability over key ranges.
  */
class PKCondition
{
public:
    /// Does not take into account the SAMPLE section. all_columns - the set of all columns of the table.
    PKCondition(
        const SelectQueryInfo & query_info,
        const Context & context,
        const NamesAndTypesList & all_columns,
        const SortDescription & sort_descr,
        const ExpressionActionsPtr & pk_expr);

    /// Whether the condition is feasible in the key range.
    /// left_pk and right_pk must contain all fields in the sort_descr in the appropriate order.
    /// data_types - the types of the primary key columns.
    bool mayBeTrueInRange(size_t used_key_size, const Field * left_pk, const Field * right_pk, const DataTypes & data_types) const;

    /// Is the condition valid in a semi-infinite (not limited to the right) key range.
    /// left_pk must contain all the fields in the sort_descr in the appropriate order.
    bool mayBeTrueAfter(size_t used_key_size, const Field * left_pk, const DataTypes & data_types) const;

    /// Checks that the index can not be used.
    bool alwaysUnknownOrTrue() const;

    /// Get the maximum number of the primary key element used in the condition.
    size_t getMaxKeyColumn() const;

    /// Impose an additional condition: the value in the column column must be in the `range` range.
    /// Returns whether there is such a column in the primary key.
    bool addCondition(const String & column, const Range & range);

    String toString() const;


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
        size_t key_column;
        /// For FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
        ASTPtr in_function;
        using MergeTreeSetIndexPtr = std::shared_ptr<MergeTreeSetIndex>;
        MergeTreeSetIndexPtr set_index;

        /** A chain of possibly monotone functions.
          * If the primary key column is wrapped in functions that can be monotonous in some value ranges
          * (for example: -toFloat64(toDayOfWeek(date))), then here the functions will be located: toDayOfWeek, toFloat64, negate.
          */
        using MonotonicFunctionsChain = std::vector<FunctionBasePtr>;
        mutable MonotonicFunctionsChain monotonic_functions_chain;    /// The function execution does not violate the constancy.
    };

    static Block getBlockWithConstants(
        const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns);

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value, const ASTPtr & node)>;
    static const AtomMap atom_map;

    static std::optional<Range> applyMonotonicFunctionsChainToRange(
        Range key_range,
        RPNElement::MonotonicFunctionsChain & functions,
        DataTypePtr current_type);

private:
    using RPN = std::vector<RPNElement>;
    using ColumnIndices = std::map<String, size_t>;

    bool mayBeTrueInRange(
        size_t used_key_size,
        const Field * left_pk,
        const Field * right_pk,
        const DataTypes & data_types,
        bool right_bounded) const;

    bool mayBeTrueInRangeImpl(const std::vector<Range> & key_ranges, const DataTypes & data_types) const;

    void traverseAST(const ASTPtr & node, const Context & context, Block & block_with_constants);
    bool atomFromAST(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out);
    bool operatorFromAST(const ASTFunction * func, RPNElement & out);

    /** Is node the primary key column
      *  or expression in which column of primary key is wrapped by chain of functions,
      *  that can be monotomic on certain ranges?
      * If these conditions are true, then returns number of column in primary key, type of resulting expression
      *  and fills chain of possibly-monotonic functions.
      */
    bool isPrimaryKeyPossiblyWrappedByMonotonicFunctions(
        const ASTPtr & node,
        const Context & context,
        size_t & out_primary_key_column_num,
        DataTypePtr & out_primary_key_res_column_type,
        RPNElement::MonotonicFunctionsChain & out_functions_chain);

    bool isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const ASTPtr & node,
        size_t & out_primary_key_column_num,
        DataTypePtr & out_primary_key_column_type,
        std::vector<const ASTFunction *> & out_functions_chain);

    bool canConstantBeWrappedByMonotonicFunctions(
        const ASTPtr & node,
        size_t & out_primary_key_column_num,
        DataTypePtr & out_primary_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    void getPKTuplePositionMapping(
        const ASTPtr & node,
        const Context & context,
        std::vector<MergeTreeSetIndex::PKTuplePositionMapping> & indexes_mapping,
        const size_t tuple_index,
        size_t & out_primary_key_column_num);

    bool isTupleIndexable(
        const ASTPtr & node,
        const Context & context,
        RPNElement & out,
        const SetPtr & prepared_set,
        size_t & out_primary_key_column_num);

    RPN rpn;

    SortDescription sort_descr;
    ColumnIndices pk_columns;
    ExpressionActionsPtr pk_expr;
    PreparedSets prepared_sets;
};

}
