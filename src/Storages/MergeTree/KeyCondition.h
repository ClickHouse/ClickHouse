#pragma once

#include <optional>

#include <Core/SortDescription.h>
#include <Core/Range.h>

#include <DataTypes/Serializations/ISerialization.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TreeRewriter.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/BoolMask.h>
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
class MergeTreeSetIndex;


/// Canonize the predicate
/// * push down NOT to leaf nodes
/// * remove aliases and re-generate function names
/// * remove unneeded functions (e.g. materialize)
struct ActionsDAGWithInversionPushDown
{
    std::optional<ActionsDAG> dag;
    const ActionsDAG::Node * predicate = nullptr;

    explicit ActionsDAGWithInversionPushDown(const ActionsDAG::Node * predicate_, const ContextPtr & context);
};

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
private:
    struct ThisIsPrivate {};

public:
    /// Construct key condition from ActionsDAG nodes
    KeyCondition(
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false);

    struct BloomFilterData
    {
        using HashesForColumns = std::vector<std::vector<uint64_t>>;
        HashesForColumns hashes_per_column;
        /// Subset of RPNElement::key_columns.
        std::vector<std::size_t> key_columns;
    };

    struct BloomFilter
    {
        virtual ~BloomFilter() = default;

        virtual bool findAnyHash(const std::vector<uint64_t> & hashes) = 0;
    };

    using ColumnIndexToBloomFilter = std::unordered_map<std::size_t, std::unique_ptr<BloomFilter>>;
    /// Whether the condition and its negation are feasible in the direct product of single column ranges specified by `hyperrectangle`.
    BoolMask checkInHyperrectangle(
        const Hyperrectangle & hyperrectangle,
        const DataTypes & data_types,
        const ColumnIndexToBloomFilter & column_index_to_column_bf = {}) const;

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

    size_t getNumKeyColumns() const { return num_key_columns; }

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
        struct Polygon;

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
            /// Special for pointInPolygon to utilize minmax indices.
            /// For example: pointInPolygon((x, y), [(0, 0), (0, 2), (2, 2), (2, 0)])
            FUNCTION_POINT_IN_POLYGON,
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

        RPNElement();
        explicit RPNElement(Function function_);
        RPNElement(Function function_, std::vector<size_t> key_columns_);
        RPNElement(Function function_, std::vector<size_t> key_columns_, const Range & range_);

        /// If `key_names` is empty, prints column numbers instead.
        String toString(const std::vector<String> & key_names = {}) const;

        size_t getKeyColumn() const { chassert(key_columns.size() == 1); return key_columns.at(0); }

        Function function = FUNCTION_UNKNOWN;

        /// Whether to relax the key condition (e.g., for LIKE queries without a perfect prefix).
        bool relaxed = false;

        /// For FUNCTION_IN_RANGE and FUNCTION_NOT_IN_RANGE.
        Range range = Range::createWholeUniverse();

        /// Which columns are involved. E.g.:
        ///  * if FUNCTION[_NOT]_IN_RANGE: exactly one element,
        ///  * if FUNCTION[_NOT]_IN_SET: one or more elements in nondecreasing order, same as
        ///    set_index->getIndexesMapping()[..].key_index,
        ///  * if FUNCTION_POINT_IN_POLYGON: two elements (x, y) describing the point,
        ///    as in pointInPolygon((x, y), ...).
        std::vector<size_t> key_columns;

        /// If a key column is a space filling curve, e.g. mortonEncode(x, y),
        /// we will analyze expressions of its arguments (x and y) similarly how we do for normal
        /// key columns. This field designates the argument number (0 for x, 1 for y), while
        /// key_columns[0] points to the encoded column like mortonEncode(x, y).
        /// Normally this field is only used during KeyCondition construction; by the end of
        /// construction, such RPNElements get converted to FUNCTION_ARGS_IN_HYPERRECTANGLE operating
        /// on the key column directly (see findHyperrectanglesForArgumentsOfSpaceFillingCurves).
        std::optional<size_t> argument_num_of_space_filling_curve;

        /// For FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
        using MergeTreeSetIndexPtr = std::shared_ptr<const MergeTreeSetIndex>;
        MergeTreeSetIndexPtr set_index;

        /// For FUNCTION_ARGS_IN_HYPERRECTANGLE
        Hyperrectangle space_filling_curve_args_hyperrectangle;

        /// For FUNCTION_POINT_IN_POLYGON.
        /// Function name (e.g. 'pointInPolygon') and the polygon.
        /// Additionally, `key_columns` has two elements for point coordinates (x, y).
        std::optional<String> point_in_polygon_function_name;
        std::shared_ptr<Polygon> polygon;

        /// What functions are applied to the key column before doing the range/set/etc check.
        /// E.g. toDate(key) > '2025-09-12'.
        /// Applicable only for some FUNCTION_* types and only if key_columns.size() == 1.
        MonotonicFunctionsChain monotonic_functions_chain;

        std::optional<BloomFilterData> bloom_filter_data;
    };

    using RPN = std::vector<RPNElement>;
    using ColumnIndices = std::map<String, size_t>;

    using AtomMap = std::unordered_map<std::string, bool(*)(RPNElement & out, const Field & value)>;
    static const AtomMap atom_map;

    const RPN & getRPN() const { return rpn; }
    const ColumnIndices & getKeyColumns() const { return key_columns; }

    bool isRelaxed() const { return relaxed; }

    bool isSinglePoint() const { return single_point; }

    void prepareBloomFilterData(std::function<std::optional<uint64_t>(size_t column_idx, const Field &)> hash_one,
                                std::function<std::optional<std::vector<uint64_t>>(size_t column_idx, const ColumnPtr &)> hash_many);

    /// Split the KeyCondition into single-column conditions AND-ed together, plus a remaining
    /// multi-column KeyCondition.
    /// E.g. `x AND (y OR z) AND w` is split into out_column_conditions = {`x`, `w`}, out_complex_condition = {`y OR z`}.
    ///
    /// All returned KeyCondition-s use the same column numbering and have the same getNumKeyColumns()
    /// as the original KeyCondition. E.g. when calling checkInHyperrectangle on the single-column
    /// KeyCondition-s, the passed hyperrectangle must have as many elements as the original key size,
    /// not just one element.
    void extractSingleColumnConditions(std::vector<std::pair</*column_idx*/ size_t, std::shared_ptr<KeyCondition>>> & out_column_conditions, std::shared_ptr<KeyCondition> * out_complex_condition) const;

    /// List key columns that are actually used in the condition. E.g. condition `x AND y` doesn't use column `z`.
    std::unordered_set<size_t> getUsedColumns() const;

    /// Private constructor.
    KeyCondition(
        ThisIsPrivate,
        ColumnIndices key_columns_,
        size_t num_key_columns_,
        bool single_point_,
        bool date_time_overflow_behavior_ignore_,
        bool relaxed_);

private:
    /// Information used when building a KeyCondition out of ActionsDAG.
    struct BuildInfo
    {
        /// Expression which is used for key condition.
        const ExpressionActionsPtr key_expr;
        /// All intermediate columns are used to calculate key_expr.
        const NameSet key_subexpr_names;
    };

    BoolMask checkInRange(
        size_t used_key_size,
        const FieldRef * left_key,
        const FieldRef * right_key,
        const DataTypes & data_types,
        bool right_bounded,
        BoolMask initial_mask) const;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, const BuildInfo & info, RPNElement & out);

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
        const BuildInfo & info,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        bool assume_function_monotonicity = false);

    bool isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_column_type,
        std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain);

    bool extractMonotonicFunctionsChainFromKey(
        ContextPtr context,
        const String & expr_name,
        const BuildInfo & info,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const;

    bool canConstantBeWrappedByMonotonicFunctions(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
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
        const BuildInfo & info,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain);

    /// If it's possible to make an RPNElement
    /// that will filter values (possibly tuples) by the content of 'prepared_set',
    /// do it and return true.
    bool tryPrepareSetIndex(
        const RPNBuilderFunctionTreeNode & func,
        const BuildInfo & info,
        RPNElement & out,
        bool allow_constant_transformation);

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

    void getAllSpaceFillingCurves(const BuildInfo & info);

    /// Determines if a function maintains monotonicity.
    /// Currently only does special checks for toDateTime monotonicity.
    bool isFunctionReallyMonotonic(const IFunctionBase & func, const IDataType & arg_type) const;

    /// Returns the ranges in `rpn` corresponding to subconditions that are AND-ed together.
    /// E.g. consider condition `x AND (y OR z) AND w`. The `rpn` is [x, y, z, OR, AND, w, AND].
    /// This function will return [(0, 1), (1, 4), (5, 6)], corresponding to: [x], [y, z, OR], [w].
    /// The returned vector is not necessarily sorted.
    std::vector<std::pair</*start*/ size_t, /*end*/ size_t>> topLevelConjunction() const;

    RPN rpn;

    /// If query has no filter, rpn will has one element with unknown function.
    /// This flag identify whether there are filters.
    bool has_filter;

    ColumnIndices key_columns;
    /// `key_columns` may contain all columns of the key tuple or only the columns used in the
    /// KeyCondition. Either way, num_key_columns is the length of the whole key tuple.
    size_t num_key_columns = 0;

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

    /// If true, this key condition is used only to validate single value
    /// ranges. It permits key_expr and constant of FunctionEquals to be
    /// transformed by any deterministic functions. It is used by
    /// PartitionPruner.
    bool single_point;

    /// Holds the result of (setting.date_time_overflow_behavior == DateTimeOverflowBehavior::Ignore)
    /// Used to check toDateTime monotonicity.
    bool date_time_overflow_behavior_ignore;

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
}
