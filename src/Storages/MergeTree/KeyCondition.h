#pragma once

#include <optional>

#include <Core/SortDescription.h>
#include <Core/Range.h>
#include <Core/PlainRanges.h>

#include <DataTypes/Serializations/ISerialization.h>

#include <Parsers/ASTExpressionList.h>

#include <Interpreters/Set.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/RPNBuilder.h>


namespace DB
{

class ASTFunction;
class Context;
class IFunction;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct ActionDAGNodes;


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
    /// Construct key condition from ActionsDAG nodes
    KeyCondition(
        const ActionsDAG * filter_dag,
        ContextPtr context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false);

    /// Whether the condition and its negation are feasible in the direct product of single column ranges specified by `hyperrectangle`.
    BoolMask checkInHyperrectangle(
        const Hyperrectangle & hyperrectangle,
        const DataTypes & data_types) const;

    /// Whether the condition and its negation are (independently) feasible in the key range.
    /// left_key and right_key must contain all fields in the sort_descr in the appropriate order.
    /// data_types - the types of the key columns.
    /// Argument initial_mask is used for early exiting the implementation when we do not care about
    /// one of the resulting mask components (see BoolMask::consider_only_can_be_XXX).
    BoolMask checkInRange(
        size_t used_key_size,
        const FieldRef * left_keys,
        const FieldRef * right_keys,
        const DataTypes & data_types,
        BoolMask initial_mask = BoolMask(false, false)) const;

    /// Same as checkInRange, but calculate only may_be_true component of a result.
    /// This is more efficient than checkInRange(...).can_be_true.
    bool mayBeTrueInRange(
        size_t used_key_size,
        const FieldRef * left_keys,
        const FieldRef * right_keys,
        const DataTypes & data_types) const;

    /// Checks that the index can not be used
    /// FUNCTION_UNKNOWN will be AND'ed (if any).
    bool alwaysUnknownOrTrue() const;

    /// Checks that the index can not be used
    /// Does not allow any FUNCTION_UNKNOWN (will instantly return true).
    bool anyUnknownOrAlwaysTrue() const;

    bool alwaysFalse() const;

    bool hasMonotonicFunctionsChain() const;

    /// Impose an additional condition: the value in the column `column` must be in the range `range`.
    /// Returns whether there is such a column in the key.
    bool addCondition(const String & column, const Range & range);

    String toString() const;

    /// Get the key indices of key names used in the condition.
    const std::vector<size_t> & getKeyIndices() const { return key_indices; }

    /// Condition description for EXPLAIN query.
    struct Description
    {
        /// Which columns from PK were used, in PK order.
        std::vector<std::string> used_keys;
        /// Condition which was applied, mostly human-readable.
        std::string condition;
    };

    Description getDescription() const;

    /** A chain of possibly monotone functions.
      * If the key column is wrapped in functions that can be monotonous in some value ranges
      * (for example: -toFloat64(toDayOfWeek(date))), then here the functions will be located: toDayOfWeek, toFloat64, negate.
      */
    using MonotonicFunctionsChain = std::vector<FunctionBasePtr>;

    /** Computes value of constant expression and its data type.
      * Returns false, if expression isn't constant.
      */
    static bool getConstant(
        const ASTPtr & expr,
        Block & block_with_constants,
        Field & out_value,
        DataTypePtr & out_type);

    /** Calculate expressions, that depend only on constants.
      * For index to work when something like "WHERE Date = toDate(now())" is written.
      */
    static Block getBlockWithConstants(
        const ASTPtr & query,
        const TreeRewriterResultPtr & syntax_analyzer_result,
        ContextPtr context);

    static std::optional<Range> applyMonotonicFunctionsChainToRange(
        Range key_range,
        const MonotonicFunctionsChain & functions,
        DataTypePtr current_type,
        bool single_point = false);

    static ActionsDAG cloneASTWithInversionPushDown(ActionsDAG::NodeRawConstPtrs nodes, const ContextPtr & context);

    bool matchesExactContinuousRange() const;

    /// Extract plain ranges of the condition.
    /// Note that only support one column key condition.
    ///
    /// Now some cases are parsed to unknown function:
    ///     1. where 1=1
    ///     2. where true
    ///     3. no where
    /// TODO handle the cases when generate RPN.
    bool extractPlainRanges(Ranges & ranges) const;

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
            FUNCTION_IS_NULL,
            FUNCTION_IS_NOT_NULL,
            /// Special for space-filling curves.
            /// For example, if key is mortonEncode(x, y),
            /// and the condition contains its arguments, e.g.:
            ///   x >= 10 AND x <= 20 AND y >= 20 AND y <= 30,
            /// this expression will be analyzed and then represented by following:
            ///   args in hyperrectangle [10, 20] × [20, 30].
            FUNCTION_ARGS_IN_HYPERRECTANGLE,
            /// Can take any value.
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement() = default;
        RPNElement(Function function_) : function(function_) {} /// NOLINT
        RPNElement(Function function_, size_t key_column_) : function(function_), key_column(key_column_) {}
        RPNElement(Function function_, size_t key_column_, const Range & range_)
            : function(function_), range(range_), key_column(key_column_) {}

        String toString() const;
        String toString(std::string_view column_name, bool print_constants) const;

        Function function = FUNCTION_UNKNOWN;

        /// For FUNCTION_IN_RANGE and FUNCTION_NOT_IN_RANGE.
        Range range = Range::createWholeUniverse();
        size_t key_column = 0;

        /// If the key_column is a space filling curve, e.g. mortonEncode(x, y),
        /// we will analyze expressions of its arguments (x and y) similarly how we do for a normal key columns,
        /// and this designates the argument number (0 for x, 1 for y):
        std::optional<size_t> argument_num_of_space_filling_curve;

        /// For FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
        using MergeTreeSetIndexPtr = std::shared_ptr<const MergeTreeSetIndex>;
        MergeTreeSetIndexPtr set_index;

        /// For FUNCTION_ARGS_IN_HYPERRECTANGLE
        Hyperrectangle space_filling_curve_args_hyperrectangle;

        MonotonicFunctionsChain monotonic_functions_chain;
    };

    using RPN = std::vector<RPNElement>;
    using ColumnIndices = std::map<String, size_t>;

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value)>;
    static const AtomMap atom_map;

    const RPN & getRPN() const { return rpn; }
    const ColumnIndices & getKeyColumns() const { return key_columns; }

    bool isRelaxed() const { return relaxed; }

private:
    BoolMask checkInRange(
        size_t used_key_size,
        const FieldRef * left_key,
        const FieldRef * right_key,
        const DataTypes & data_types,
        bool right_bounded,
        BoolMask initial_mask) const;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    /// Is node the key column, or an argument of a space-filling curve that is a key column,
    ///  or expression in which that column is wrapped by a chain of functions,
    ///  that can be monotonic on certain ranges?
    /// If these conditions are true, then returns number of column in key,
    ///  optionally the argument position of a space-filling curve,
    ///  type of resulting expression
    ///  and fills chain of possibly-monotonic functions.
    /// If @assume_function_monotonicity = true, assume all deterministic
    /// functions as monotonic, which is useful for partition pruning.
    bool isKeyPossiblyWrappedByMonotonicFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        bool assume_function_monotonicity = false);

    bool isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_column_type,
        std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain);

    bool extractMonotonicFunctionsChainFromKey(
        ContextPtr context,
        const String & expr_name,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const;

    bool canConstantBeWrappedByMonotonicFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    /// Checks if node is a subexpression of any of key columns expressions,
    /// wrapped by deterministic functions, and if so, returns `true`, and
    /// specifies key column position / type. Besides that it produces the
    /// chain of functions which should be executed on set, to transform it
    /// into key column values.
    bool canSetValuesBeWrappedByFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain);

    /// If it's possible to make an RPNElement
    /// that will filter values (possibly tuples) by the content of 'prepared_set',
    /// do it and return true.
    bool tryPrepareSetIndex(
        const RPNBuilderFunctionTreeNode & func,
        RPNElement & out,
        size_t & out_key_column_num,
        bool & is_constant_transformed);

    /// Checks that the index can not be used.
    ///
    /// If unknown_any is false (used by alwaysUnknownOrTrue()), then FUNCTION_UNKNOWN can be AND'ed,
    /// otherwise (anyUnknownOrAlwaysTrue()) first FUNCTION_UNKNOWN will return true (index cannot be used).
    ///
    /// Consider the following example:
    ///
    ///     CREATE TABLE test(p DateTime, k int) ENGINE MergeTree PARTITION BY toDate(p) ORDER BY k;
    ///     INSERT INTO test VALUES ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);
    ///
    /// - SELECT count() FROM test WHERE toDate(p) >= '2020-09-01' AND p <= '2020-09-01 00:00:00'
    ///   In this case rpn will be (FUNCTION_IN_RANGE, FUNCTION_UNKNOWN (due to strict), FUNCTION_AND)
    ///   and for optimize_trivial_count_query we cannot use index if there is at least one FUNCTION_UNKNOWN.
    ///   since there is no post processing and return count() based on only the first predicate is wrong.
    ///
    /// - SELECT * FROM test WHERE toDate(p) >= '2020-09-01' AND p <= '2020-09-01 00:00:00'
    ///   In this case will be (FUNCTION_IN_RANGE, FUNCTION_IN_RANGE (due to non-strict), FUNCTION_AND)
    ///   so it will prune everything out and nothing will be read.
    ///
    /// - SELECT * FROM test WHERE toDate(p) >= '2020-09-01' AND toUnixTimestamp(p)%5==0
    ///   In this case will be (FUNCTION_IN_RANGE, FUNCTION_UNKNOWN, FUNCTION_AND)
    ///   and all, two, partitions will be scanned, but due to filtering later none of rows will be matched.
    bool unknownOrAlwaysTrue(bool unknown_any) const;

    /** Iterates over RPN and collapses FUNCTION_IN_RANGE over the arguments of space-filling curve function
      * into atom of type FUNCTION_ARGS_IN_HYPERRECTANGLE.
      */
    void findHyperrectanglesForArgumentsOfSpaceFillingCurves();

    RPN rpn;

    /// If query has no filter, rpn will has one element with unknown function.
    /// This flag identify whether there are filters.
    bool has_filter;

    ColumnIndices key_columns;
    std::vector<size_t> key_indices;

    /// Expression which is used for key condition.
    const ExpressionActionsPtr key_expr;
    /// All intermediate columns are used to calculate key_expr.
    const NameSet key_subexpr_names;

    /// Space-filling curves in the key
    enum class SpaceFillingCurveType
    {
        Unknown = 0,
        Morton,
        Hilbert
    };
    static const std::unordered_map<String, SpaceFillingCurveType> space_filling_curve_name_to_type;

    struct SpaceFillingCurveDescription
    {
        size_t key_column_pos;
        String function_name;
        std::vector<String> arguments;
        SpaceFillingCurveType type;
    };
    using SpaceFillingCurveDescriptions = std::vector<SpaceFillingCurveDescription>;
    SpaceFillingCurveDescriptions key_space_filling_curves;
    void getAllSpaceFillingCurves();

    /// Array joined column names
    NameSet array_joined_column_names;

    /// If true, this key condition is used only to validate single value
    /// ranges. It permits key_expr and constant of FunctionEquals to be
    /// transformed by any deterministic functions. It is used by
    /// PartitionPruner.
    bool single_point;

    /// If true, this key condition is relaxed. When a key condition is relaxed, it
    /// is considered weakened. This is because keys may not always align perfectly
    /// with the condition specified in the query, and the aim is to enhance the
    /// usefulness of different types of key expressions across various scenarios.
    ///
    /// For instance, in a scenario with one granule of key column toDate(a), where
    /// the hyperrectangle is toDate(a) ∊ [x, y], the result of a ∊ [u, v] can be
    /// deduced as toDate(a) ∊ [toDate(u), toDate(v)] due to the monotonic
    /// non-decreasing nature of the toDate function. Similarly, for a ∊ (u, v), the
    /// transformed outcome remains toDate(a) ∊ [toDate(u), toDate(v)] as toDate
    /// does not strictly follow a monotonically increasing transformation. This is
    /// one of the main use case about key condition relaxation.
    ///
    /// During the KeyCondition::checkInRange process, relaxing the key condition
    /// can lead to a loosened result. For example, when transitioning from (u, v)
    /// to [u, v], if a key is within the range [u, u], BoolMask::can_be_true will
    /// be true instead of false, causing us to not skip this granule. This behavior
    /// is acceptable as we can still filter it later on. Conversely, if the key is
    /// within the range [u, v], BoolMask::can_be_false will be false instead of
    /// true, indicating a stricter condition where all elements of the granule
    /// satisfy the key condition. Hence, when the key condition is relaxed, we
    /// cannot rely on BoolMask::can_be_false. One significant use case of
    /// BoolMask::can_be_false is in trivial count optimization.
    ///
    /// Now let's review all the cases of key condition relaxation across different
    /// atom types.
    ///
    /// 1. Not applicable: ALWAYS_FALSE, ALWAYS_TRUE, FUNCTION_NOT,
    /// FUNCTION_AND, FUNCTION_OR.
    ///
    /// These atoms are either never relaxed or are relaxed by their children.
    ///
    /// 2. Constant transformed: FUNCTION_IN_RANGE, FUNCTION_NOT_IN_RANGE,
    /// FUNCTION_IS_NULL. FUNCTION_IS_NOT_NULL, FUNCTION_IN_SET (1 element),
    /// FUNCTION_NOT_IN_SET (1 element)
    ///
    /// These atoms are relaxed only when the associated constants undergo
    /// transformation by monotonic functions, as illustrated in the example
    /// mentioned earlier.
    ///
    /// 3. Always relaxed: FUNCTION_UNKNOWN, FUNCTION_IN_SET (>1 elements),
    /// FUNCTION_NOT_IN_SET (>1 elements), FUNCTION_ARGS_IN_HYPERRECTANGLE
    ///
    /// These atoms are always considered relaxed for the sake of implementation
    /// simplicity, as there may be "gaps" within the atom's hyperrectangle that the
    /// granule's hyperrectangle may or may not intersect.
    ///
    /// NOTE: we also need to examine special functions that generate atoms. For
    /// example, the `match` function can produce a FUNCTION_IN_RANGE atom based
    /// on a given regular expression, which is relaxed for simplicity.
    bool relaxed = false;
};

String extractFixedPrefixFromLikePattern(std::string_view like_pattern, bool requires_perfect_prefix);

}
