#pragma once

#include <optional>

#include <Core/SortDescription.h>
#include <Core/Range.h>


#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Set.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/BoolMask.h>
#include <Storages/MergeTree/KeyOrder.h>
#include <Storages/MergeTree/RPNBuilder.h>


namespace DB
{

class ASTFunction;
class Context;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct ActionDAGNodes;
class MergeTreeSetIndex;
struct KeyDescription;


/// Canonize the predicate
/// * push down NOT to leaf nodes
/// * remove aliases and re-generate function names
/// * remove unneeded functions (e.g. materialize)
struct ActionsDAGWithInversionPushDown
{
    std::optional<ActionsDAG> dag;
    const ActionsDAG::Node * predicate = nullptr;

    /// `boolean_context`: Pass true only when the caller uses `predicate_` as a filter: it tests each row for truthiness
    /// and discards the value (index analysis of a WHERE/PREWHERE). Then `cloneDAGWithInversionPushDown` may apply
    /// truthiness-preserving but value-changing rewrites (`tryRewriteCoalesceCondition`, `tryRewriteCoalesceComparison`),
    /// which can differ from the original (e.g. `NULL` vs `false`) on NULL rows but agree on truthiness.
    /// There is no correctness cost to passing false, but it may miss some optimization opportunities.
    explicit ActionsDAGWithInversionPushDown(const ActionsDAG::Node * predicate_, const ContextPtr & context, bool boolean_context);
};


struct DeterministicKeyTransformDag
{
    ExpressionActionsPtr actions;
    String output_name;
    DataTypePtr input_type;
    String input_name;
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
    /// Construct key condition from ActionsDAG nodes.
    /// This overload takes the key column names and expression without any direction information,
    /// so the condition treats the key as ascending in every column. Use it only for keys that
    /// cannot be reverse-sorted (e.g. skip index expressions, virtual row-offset columns).
    KeyCondition(
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false,
        bool skip_analysis_ = false, /// Toggled by `use_primary_key`, `use_partition_key` setting. Useful for testing.
        bool require_ready_sets_ = false); /// Analyse only already-built `IN` sets; never execute a subquery.

    /// Takes the key's `KeyDescription` and honors its per-column sort directions. An empty vector
    /// of reverse flags means all-ascending, as for a partition key. Passing only column names and
    /// expressions would analyze a reverse-sorted key as ascending.
    /// Primary-key conditions must use `createForPrimaryKey`, which also applies exactness restrictions.
    KeyCondition(
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        const KeyDescription & key_description,
        bool single_point_ = false,
        bool skip_analysis_ = false);

    /// Builds a primary-key condition with its sort directions and restrictions on range exactness.
    /// Partition and skip-index conditions use the general constructors because their range semantics differ.
    static KeyCondition createForPrimaryKey(
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        const KeyDescription & primary_key,
        bool skip_analysis = false);

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

        /// `hashes` are the hashes of the query constants of one atom for one column. They are sorted
        /// and deduplicated (see `prepareBloomFilterData`), which lets an implementation with a sorted
        /// value set intersect the two sequences in one pass instead of searching for each hash
        /// separately. Returns true if any of them may be present.
        virtual bool findAnyHash(const std::vector<uint64_t> & hashes) = 0;
    };

    using ColumnIndexToBloomFilter = std::unordered_map<std::size_t, std::unique_ptr<BloomFilter>>;

    /// Ref : https://github.com/ClickHouse/ClickHouse/pull/87781
    /// ClickHouse always supported pruning for conditions with conjunctions/ANDs :
    ///    A = 5 AND B > 10 AND C < 1000
    /// The code was oriented towards each skip index application immediately 'throwing out'
    /// ranges that do not pass the condition and moving on to evaluating the next skip index
    /// on a reduced set of ranges.
    ///
    /// But a condition with ORs is fundamentally different : A = 5 OR B = 5. If a range does
    /// not match the condition (A = 5) using skip index on A, we cannot throw out or prune away
    /// that range. We need to 'wait' for skip index B application and see the result of B = 5
    /// on that range.
    ///
    /// Range pruning for mixed AND/OR predicates uses below callback to record each atom's
    /// evaluation result got by applying corresponding skip index (true or false). This is
    /// done in MergeTreeDataSelectExecutor::filterMarksUsingIndex(). Each *IndexCondition
    /// invokes this callback as it is evaluating the predicate. The final result for each
    /// granule is computed in MergeTreeDataSelectExecutor::mergePartialResultsForDisjunctions()
    using UpdatePartialDisjunctionResultFn = std::function<void (size_t position, bool result, bool is_unknown)>;

    /// Whether the condition and its negation are feasible in the direct product of single column ranges specified by `hyperrectangle`.
    BoolMask checkInHyperrectangle(
        const Hyperrectangle & hyperrectangle,
        const DataTypes & data_types,
        const ColumnIndexToBloomFilter & column_index_to_column_bf = {},
        const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn = nullptr) const;

    /// Optimized overload. Instead of all/prefix of key columns, any subsequence of key column information (in order) can be given.
    /// `key_col_to_sparse_pos` maps key index to position in `sparse_hyperrectangle`, or -1 if not tracked.
    /// If some key column >= `key_col_to_sparse_pos`.size(), it is considered as not tracked.
    /// See the optimized overload of checkInRange for explanation of relevant parameters.
    BoolMask checkInHyperrectangle(
        const std::vector<int> & key_col_to_sparse_pos,
        const Hyperrectangle & sparse_hyperrectangle,
        const DataTypes & sparse_data_types) const;

    /// Whether the condition and its negation are (independently) feasible in the key range.
    /// left_key and right_key must contain all fields in the sort_descr in the appropriate order.
    /// data_types - the types of the key columns.
    /// Argument initial_mask is used for early exiting the implementation when we do not care about
    /// one of the resulting mask components (see BoolMask::consider_only_can_be_XXX).
    /// key_bounds - optional per-column bounds the key values are known to lie within (e.g. the part's
    /// partition minmax). A key without a bound defaults to (-inf, +inf).
    BoolMask checkInRange(
        size_t key_size,
        const FieldRef * left_keys,
        const FieldRef * right_keys,
        const DataTypes & data_types,
        BoolMask initial_mask = BoolMask(false, false),
        const Hyperrectangle * key_bounds = nullptr) const;

    /// Optimized overload. Instead of all/prefix of key columns, any subsequence of key column information (in order) can be given.
    /// However, `equal_boundaries_mask` must have the information about all/prefix keys. `equal_boundaries_mask` specifies whether ith key's
    /// left and right boundaries are equal or not.
    /// For example, suppose, a table has 6 columns in primary key : (0, 1, 2, 3, 4, 5)
    /// The caller wants to use only columns (1, 3, 4) for range check.
    /// Then, `sparse_key_indices` = {1, 3, 4}
    /// `equal_boundaries_mask` = {false, true, false, true, true, false}
    ///      Information about the entire prefix covered by `equal_boundaries_mask` must be specified.
    /// `sparse_left_keys` and `sparse_right_keys` contain only 3 fields each, corresponding to columns (1, 3, 4).
    /// `sparse_data_types` contain only 3 data types each, corresponding to columns (1, 3, 4).
    /// key_bounds - optional per-column bounds the key values are known to lie within (e.g. the part's
    /// partition minmax), indexed by full key column position. A key without a bound defaults to (-inf, +inf).
    /// `sparse_key_indices` may also contain indices >= `equal_boundaries_mask.size()` (e.g. key columns not
    /// present in the in-memory index but bounded by the part's partition minmax). Such columns are constant
    /// coordinates: their range is `(*key_bounds)[key_index]` for the whole call, they do not participate in
    /// the hyperrectangle enumeration, and their entries in `sparse_left_keys`/`sparse_right_keys` are ignored.
    BoolMask checkInRange(
        const std::vector<size_t> & sparse_key_indices,
        const FieldRef * sparse_left_keys,
        const FieldRef * sparse_right_keys,
        const DataTypes & sparse_data_types,
        const std::vector<UInt8> & equal_boundaries_mask,
        BoolMask initial_mask,
        const Hyperrectangle * key_bounds = nullptr) const;

    /// Like `checkInRange`, but exact atoms can supply `can_be_false` without the relaxation
    /// introduced by their siblings in the same predicate group. All atoms still contribute to
    /// `can_be_true` for pruning. This follows the eligibility rules of `canCheckExactness`;
    /// ineligible conditions use the ordinary range check.
    /// Components set to true in `initial_mask` are ignored by the caller and need no separate
    /// evaluation. Their returned values are unspecified, as with `checkInRange`.
    BoolMask checkInRangeWithExactness(
        size_t key_size,
        const FieldRef * left_keys,
        const FieldRef * right_keys,
        const DataTypes & data_types,
        BoolMask initial_mask = BoolMask(false, false),
        const Hyperrectangle * key_bounds = nullptr) const;

    /// Checks the sparse representation accepted by `checkInRange`, with the same exactness rules.
    BoolMask checkInRangeWithExactness(
        const std::vector<size_t> & sparse_key_indices,
        const FieldRef * sparse_left_keys,
        const FieldRef * sparse_right_keys,
        const DataTypes & sparse_data_types,
        const std::vector<UInt8> & equal_boundaries_mask,
        BoolMask initial_mask,
        const Hyperrectangle * key_bounds = nullptr) const;

    const KeyOrder & getKeyOrder() const { return key_order; }

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

    /// Returns the size of the minimal prefix of key columns that contains all columns used in the RPN.
    /// Suppose there are 5 keys columns: 0, 1, 2, 3, 4. If any RPNElement uses key columns 0, 3.
    /// Then, it returns 4 (last used key column index + 1).
    size_t getUsedKeyPrefixSize() const;

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

    /// Extract a conservative union of ranges implied by this condition for the only key column.
    ///
    /// This method tries to extract plain ranges from each top-level conjunct (AND component) independently
    /// and intersects all successfully extracted conjunct ranges, ignoring the rest.
    ///
    /// Return value semantics:
    ///  - empty vector means the condition is always false;
    ///  - a single universe range `(-Inf, +Inf)` means no bounds could be inferred;
    ///  - otherwise, the result may contain 1+ (possibly disjoint) ranges.
    ///
    /// If the key condition is not 1-dimensional (key_columns.size() != 1), the result is always `(-Inf, +Inf)`.
    ///
    /// Examples (single key column `x`):
    ///  - `x % 2 = 0 AND x < 100`                    -> { "(-Inf, 99]" }  (the `%` conjunct is ignored)
    ///  - `x > 10 AND x < 20 AND x % 2 = 0`          -> { "[11, 19]" }
    ///  - `(x BETWEEN 0 AND 3) OR (x BETWEEN 10 AND 13)` -> { "[0, 3]", "[10, 13]" }
    ///  - `x IN (8, 0, 6)`                           -> { "[0, 0]", "[6, 6]", "[8, 8]" }
    ///  - `x NOT IN (2, 4)`                          -> { "(-Inf, 1]", "[3, 3]", "[5, +Inf)" }
    ///  - `NOT (x BETWEEN 2 AND 6) AND x < 10`       -> { "(-Inf, 1]", "[7, 9]" }
    ///  - `isNull(x)`                                -> {}               (for non-nullable keys)
    ///  - `x < 5 AND x > 10`                         -> {}               (always false / contradictory)
    ///  - `x % 2 = 0`                                -> { "(-Inf, +Inf)" } (no bounds inferred)
    ///
    /// Non-examples (currently NOT extracted; result is `{ "(-Inf, +Inf)" }` unless another conjunct provides bounds):
    ///  - `x + 1 < 100`                               -> { "(-Inf, +Inf)" } (simple arithmetic on the key is not inverted to avoid potential overflow)
    ///  - `intDiv(x, 3) < 10`                         -> { "(-Inf, +Inf)" } (functions on the key are not analyzed here)
    ///  - `(x < 10 AND x % 2 = 0) OR (x < 20 AND x % 3 = 0)` -> { "(-Inf, +Inf)" } (no partial extraction across OR branches)
    Ranges extractBounds() const;

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
            /// Special for pointInPolygon to utilize primary key and minmax indices.
            /// For example: pointInPolygon((x, y), [(0, 0), (0, 2), (2, 2), (2, 0)])
            /// where x, y are key columns, or pointInPolygon(coord, [...])
            /// where coord is a key column of type Point (Tuple of two coordinates).
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

        /// Continues the preceding predicate leaf's atom group; `RPNBuilder::appendAtomGroup`
        /// defines its layout. The whole group occupies one position in the RPN built with an empty key
        /// (`key_condition_rpn_template`), which the skip-index disjunction machinery uses for positions
        /// (see `KeyCondition::checkInHyperrectangle` and
        /// `mergePartialResultsForDisjunctions`).
        bool continues_multi_atom_group = false;

        /// For FUNCTION_IN_RANGE and FUNCTION_NOT_IN_RANGE.
        Range range = Range::createWholeUniverse();

        /// Which columns are involved. E.g.:
        ///  * if FUNCTION[_NOT]_IN_RANGE: exactly one element,
        ///  * if FUNCTION[_NOT]_IN_SET: one or more elements in nondecreasing order, same as
        ///    set_index->getIndexesMapping()[..].key_index,
        ///  * if FUNCTION_POINT_IN_POLYGON: two elements (x, y) describing the point,
        ///    as in pointInPolygon((x, y), ...), or one element if the point is a whole
        ///    key column of type Tuple of two coordinates, as in pointInPolygon(coord, ...).
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
        /// `key_columns` has two elements for the point coordinates (x, y),
        /// or one element if the point is a whole key column of Tuple type.
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

    /// Whether this key condition is relaxed (computed from the RPN atoms). When a key
    /// condition is relaxed, it is considered weakened. This is because keys may not
    /// always align perfectly with the condition specified in the query, and the aim is
    /// to enhance the usefulness of different types of key expressions across various
    /// scenarios.
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
    /// These atoms are relaxed when the associated constants undergo
    /// transformation by monotonic functions, as illustrated in the example
    /// mentioned earlier, and a right-unbounded FUNCTION_IN_RANGE atom is also
    /// relaxed when its primary-key column can hold a NaN inside a `Tuple`.
    /// `createForPrimaryKey` applies this restriction before publishing the condition.
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
    /// on a given regular expression. Such an atom is relaxed unless the regular
    /// expression has a perfect or an exact prefix, e.g. "^abc.*" or "^abc$".
    bool isRelaxed() const;

    /// Reports whether range checks can prove that all rows in a range match. This is eligibility,
    /// not a result for a particular range; `checkInRangeWithExactness` evaluates the range itself.
    /// A multi-atom group can have an exact atom whose relaxed siblings force `can_be_false` to
    /// `true` and make `isRelaxed` return `true`. Removing those siblings can enable exactness while the
    /// full condition retains their stronger pruning. Uncovered relaxed atoms and multi-value set
    /// indexes remain subject to the conservative `isRelaxed` contract.
    bool canCheckExactness() const { return exactness_condition || !isRelaxed(); }

    bool isSinglePoint() const { return single_point; }

    /// Does the filter condition have any ORs?
    bool hasOnlyConjunctions() const;

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

    std::vector<size_t> getUsedColumnsInOrder() const;

    /// Private constructor.
    KeyCondition(
        ThisIsPrivate,
        ColumnIndices key_columns_,
        size_t num_key_columns_,
        bool single_point_,
        bool date_time_overflow_behavior_ignore_);

private:
    /// Whether any atom reads a `Nullable` key column whose analysed range may hold a NULL value.
    /// A NULL satisfies neither a comparison nor its negation, which the two-valued range algebra of
    /// `checkInHyperrectangle` cannot express, so it costs the analysis its `can_be_false` claim.
    bool mayReadNullKeyValue(const Hyperrectangle & hyperrectangle, const DataTypes & key_types) const;
    bool mayReadNullKeyValue(
        const std::vector<int> & key_col_to_sparse_pos,
        const Hyperrectangle & sparse_hyperrectangle,
        const DataTypes & sparse_key_types) const;

    using AtomGroup = RPNBuilder<RPNElement>::AtomGroup;

    /// Information used when building a KeyCondition out of ActionsDAG.
    struct BuildInfo
    {
        /// Expression which is used for key condition.
        const ExpressionActionsPtr key_expr;
        /// All intermediate columns are used to calculate key_expr.
        const NameSet key_subexpr_names;
        /// If true, an `IN` atom whose set is not built yet is declined instead of building it.
        /// Analysis passes that are not allowed to execute a user subquery set this.
        const bool require_ready_sets = false;
    };

    /** Atom extraction maps predicates onto key columns in two opposite directions,
      * and the naming below follows that split.
      *
      * The predicate-side functions (`tryMatch...`, `analyzePredicate...`) walk the
      * predicate expression and ask whether it is a key column, possibly wrapped in a
      * chain of functions. The discovered chain is a check-time chain: it is stored in
      * the atom (`RPNElement::monotonic_functions_chain`) and is applied to granule key
      * ranges during evaluation.
      *
      * The key-side functions (`collectKeyWrapping...`) walk the table's key expression
      * and ask which key columns can be computed from a given predicate column, and
      * how. The discovered chains and DAGs are build-time recipes: they are applied to
      * the predicate's constant (or to the set elements) once, during construction, and
      * they are never stored in atoms. The `transformConstantBy...KeyFunctions` pair is
      * the predicate-side consumer of these recipes.
      */

    /// A key-side recipe that describes how the key column `key_column_num` computes
    /// from a predicate column, as a chain of (possibly curried, see
    /// `FunctionWithOptionalConstArg`) single-argument functions. The chain is applied
    /// to the predicate's constant at build time and is never stored in atoms.
    struct KeyWrappingChain
    {
        size_t key_column_num = 0;
        DataTypePtr key_column_type;
        MonotonicFunctionsChain functions_chain;
        /// Cumulative monotonicity direction of `functions_chain`: false when applying the
        /// chain reverses comparison order (an odd number of non-increasing functions).
        bool chain_is_positive = true;
    };

    /// The result of pushing the predicate's constant through a key-side recipe.
    /// The transformed `value` and `type` live in the key space of `key_column_num`
    /// and compare against that column directly. An exact atom requires an injective
    /// transform whose input contains no NaN; otherwise the atom must be relaxed.
    struct TransformedConstant
    {
        size_t key_column_num = 0;
        DataTypePtr key_column_type;
        Field value;
        DataTypePtr type;
        /// True when this transformation requires a relaxed atom. Otherwise, subsequent type
        /// conversion and atom construction still determine whether the atom is exact.
        bool requires_relaxed_atom = true;
        /// True when the key-side chain that produced this constant reverses comparison
        /// order; the consumer must reverse the comparison operator accordingly.
        bool reverse_comparison = false;
    };

    /// A comparison candidate identifies a matched key expression and a constant to compare with it.
    struct ComparisonAtomCandidate
    {
        size_t key_column_num = 0;
        /// This is the type of the matched key expression after `monotonic_functions_chain` is applied;
        /// the comparison happens in this type.
        DataTypePtr key_expr_type;
        /// The check-time chain is stored in the atom and is applied to granule key
        /// ranges during evaluation.
        MonotonicFunctionsChain monotonic_functions_chain;
        std::optional<size_t> argument_num_of_space_filling_curve;
        Field const_value;
        DataTypePtr const_type;
        /// This flag is true when the transformed constant already describes a superset of matching values.
        /// `tryBuildComparisonAtom` can further relax the constraint during type conversion; exact
        /// conversions preserve this initial precision.
        bool is_relaxed = false;
        /// This flag is true when the key-side chain that produced the constant reverses comparison
        /// order (see `TransformedConstant::reverse_comparison`); the comparison operator
        /// must then be reversed as well.
        bool reverse_comparison = false;
    };

    /// The `extractAtoms*` family fills `group` with the atoms of one predicate leaf.
    /// A comparison like `ts >= X` may produce atoms for `toYYYYMM(ts)`, `toDate(ts)` and `ts`
    /// at once; a set atom may itself constrain several key columns. `RPNBuilder` combines the
    /// group's atoms with `AND` (emitting `atom0 atom1 AND atom2 AND ...`). An empty group means
    /// that the leaf could not be analyzed; such a leaf becomes `FUNCTION_UNKNOWN`.
    void extractAtomsFromTree(const RPNBuilderTreeNode & node, const BuildInfo & info, AtomGroup & group);
    void extractAtomsFromFunction(const RPNBuilderTreeNode & node, const BuildInfo & info, AtomGroup & group);
    void extractAtomsFromConstant(const RPNBuilderTreeNode & node, AtomGroup & group);
    /// A bare numeric column used directly as a boolean condition (`WHERE flag`) is analyzed as
    /// the comparison `flag != 0`, which may produce several atoms, including for derived keys.
    void extractBareColumnAtoms(const RPNBuilderTreeNode & node, const BuildInfo & info, AtomGroup & group);
    void extractPointInPolygonAtom(const RPNBuilderFunctionTreeNode & func, const BuildInfo & info, AtomGroup & group);
    /// `rewritten_const_value` overrides the constant operand of the comparison, for a
    /// predicate whose constant is not one of the function arguments as written (`LIKE
    /// pattern ESCAPE 'c'`, where the escape character is folded into the pattern). The key
    /// expression is then the first argument.
    void extractBinaryComparisonAtoms(
        const RPNBuilderFunctionTreeNode & func,
        const BuildInfo & info,
        const std::string & func_name,
        bool allow_relaxed_pruning,
        AtomGroup & group,
        const Field * rewritten_const_value = nullptr,
        const DataTypePtr & rewritten_const_type = nullptr);
    /// `key <=> NULL` is "key IS NULL", so it produces the `isNull` atom, but only for a
    /// bare key column: that atom ignores the monotonic-functions chain, which would be
    /// unsound for a wrapped key.
    void extractIsNullAtomForNotDistinctFrom(
        const RPNBuilderTreeNode & key_arg,
        const BuildInfo & info,
        const Field & const_value,
        AtomGroup & group);
    /// The shared core of comparison-atom extraction; the comparison is already in
    /// `key_expr <op> const` form. `constant` holds a `ColumnConst` whose value is neither NULL nor NaN.
    void extractComparisonAtomsForKeyArgument(
        const RPNBuilderTreeNode & key_arg,
        const BuildInfo & info,
        const std::string & func_name,
        const ColumnWithTypeAndName & constant,
        bool allow_relaxed_pruning,
        AtomGroup & group);

    /// Collects direct candidates, then candidates from monotonic and deterministic constant transforms.
    std::vector<ComparisonAtomCandidate> collectComparisonAtomCandidates(
        const RPNBuilderTreeNode & key_arg,
        const BuildInfo & info,
        const std::string & func_name,
        const ColumnWithTypeAndName & constant,
        bool allow_relaxed_pruning);

    /// Builds a complete comparison atom, including type conversion, relaxation and the final range.
    /// Returns `std::nullopt` when the candidate cannot supply a sound constraint.
    std::optional<RPNElement> tryBuildComparisonAtom(
        const ComparisonAtomCandidate & candidate, std::string func_name, const ContextPtr & context) const;

    /// Is node the key column, or an argument of a space-filling curve that is a key column,
    ///  or expression in which that column is wrapped by a chain of functions,
    ///  that can be monotonic on certain ranges?
    /// If these conditions are true, then returns number of column in key,
    ///  optionally the argument position of a space-filling curve,
    ///  type of resulting expression
    ///  and fills chain of possibly-monotonic functions.
    /// If @assume_function_monotonicity = true, assume all deterministic
    /// functions as monotonic, which is useful for partition pruning.
    bool tryMatchKeyColumnThroughMonotonicChain(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        bool assume_function_monotonicity = false);

    bool tryMatchKeyColumnThroughMonotonicChainImpl(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_column_type,
        std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain);

    /// The returned vector contains, for every key column, the chains of
    /// `allow_key_function`-approved functions through which that key column computes
    /// from the key subexpression `expr_name`. Each chain records whether it preserves
    /// or reverses comparison order (see `KeyWrappingChain::chain_is_positive`).
    /// When `first_match_only` is set, the search stops at the first collected chain.
    std::vector<KeyWrappingChain> collectKeyWrappingChains(
        ContextPtr context,
        const String & expr_name,
        const BuildInfo & info,
        bool first_match_only,
        std::function<bool(const IFunctionBase &, const IDataType &)> allow_key_function) const;

    /// For every key column that is computed from the predicate expression `node` by a
    /// chain of `allow_key_function`-approved monotonic functions, this function
    /// applies that chain to the constant once and returns the transformed constant.
    /// The resulting atoms compare against the key columns directly, but they are
    /// always relaxed, because monotonicity preserves order rather than exact
    /// membership. With `multiple_key_columns_per_condition` disabled, only the first
    /// such key column is considered.
    std::vector<TransformedConstant> transformConstantByMonotonicKeyFunctions(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        const ColumnWithTypeAndName & constant,
        std::function<bool(const IFunctionBase &, const IDataType &)> allow_key_function) const;

    /// This is the same transformation (including the single-key-column behavior of
    /// `multiple_key_columns_per_condition`), but through arbitrary deterministic
    /// key-expression DAGs. It is only valid for equality predicates (see the caller),
    /// because `x = c` implies `f(x) = f(c)` for any deterministic `f`, while order
    /// comparisons are not preserved.
    std::vector<TransformedConstant> transformConstantByDeterministicKeyFunctions(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        const ColumnWithTypeAndName & constant) const;

    /// This is a key-side recipe like `KeyWrappingChain`, except that the computation
    /// is an arbitrary deterministic sub-DAG instead of a chain of single-argument
    /// functions.
    struct DeterministicKeyDag
    {
        size_t key_column_num = 0;
        DataTypePtr key_column_type;
        DeterministicKeyTransformDag dag;
    };

    /// The returned vector contains a deterministic sub-DAG for every key column that
    /// can be computed from `expr_name`. When `first_match_only` is set, the search
    /// stops at the first collected sub-DAG.
    std::vector<DeterministicKeyDag> collectKeyWrappingDags(
        const String & expr_name,
        const BuildInfo & info,
        bool first_match_only) const;

    /// Finds the first key column computable from `node` through deterministic functions and
    /// returns its position, type, transformation DAG, and injectivity. The set analysis uses
    /// the DAG to transform set elements into key column values.
    bool tryGetDeterministicKeyTransform(
        const RPNBuilderTreeNode & node,
        const BuildInfo & info,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        DeterministicKeyTransformDag & out_transform,
        bool & out_is_injective) const;

    /// Appends prepared set-membership atoms for this predicate to `group`. Each atom may constrain
    /// several key columns. The caller finalizes their function kinds before `RPNBuilder` combines
    /// the group's atoms with `AND`. An empty group means the predicate could not be analyzed.
    void prepareSetAtomsForIn(
        const RPNBuilderFunctionTreeNode & func,
        const BuildInfo & info,
        AtomGroup & group,
        bool allow_relaxed_pruning);
    void prepareSetAtomsForHas(
        const RPNBuilderFunctionTreeNode & func,
        const BuildInfo & info,
        AtomGroup & group,
        bool allow_relaxed_pruning);

    /// The inputs for one set atom, with one mapping, transform, and type per matched key column.
    struct SetAtomCandidate
    {
        std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
        std::vector<std::optional<DeterministicKeyTransformDag>> set_transforming_dags;
        DataTypes key_expr_types;
        /// The number of tuple components of the predicate expression (1 for a scalar or packed tuple).
        size_t args_count = 1;
        /// A non-injective transform makes the set check a superset of the matching values.
        bool is_relaxed = false;
    };

    /// A predicate expression from which deterministic key transforms can derive set atoms.
    /// The source is a tuple component, a scalar expression, or a tuple expression as a whole.
    struct SetTransformSource
    {
        /// The position in the predicate tuple, or 0 for a scalar or whole tuple.
        size_t component = 0;
        String expr_name;
        /// Whole-tuple sources consume a single packed set column.
        bool is_whole_tuple = false;
    };

    /// Collects predicate components and the whole tuple that appear among the key subexpressions.
    /// For a scalar predicate expression, only the expression itself is considered.
    static std::vector<SetTransformSource> collectSetTransformSources(
        const RPNBuilderTreeNode & key_arg,
        const NameSet & key_subexpr_names,
        size_t args_count);

    struct SetIndexAnalysisResult
    {
        SetAtomCandidate componentwise_candidate;
        /// A tuple expression mapped onto one `Tuple`-typed key column needs a packed set column.
        std::optional<SetAtomCandidate> packed_tuple_candidate;
        /// Predicate components and whole-tuple expressions that can supply additional atoms.
        /// Their transformation DAGs are collected only when building the group from the set columns.
        std::vector<SetTransformSource> transform_sources;
    };

    /// Converts a candidate's set columns into key space and builds its `MergeTreeSetIndex`.
    /// The caller finalizes the prepared atom's function kind for the membership predicate.
    static std::optional<RPNElement> tryPrepareSetAtom(
        const Columns & set_columns,
        const DataTypes & set_types,
        SetAtomCandidate candidate,
        bool allow_relaxed_pruning,
        const DataTypePtr & has_element_type);

    /// Maps the predicate expression whose values are tested for set membership (the left-hand side
    /// of `IN`, or the element argument of `has`) onto key columns. Each matched tuple component has
    /// one `KeyTuplePositionMapping`, reaching a key column through a monotonic chain or a deterministic
    /// set-transforming DAG. The result also identifies source expressions for additional wrapped-set
    /// atoms, when allowed.
    /// Returns no result when there are no candidate mappings or source expressions. The set is not materialized.
    std::optional<SetIndexAnalysisResult> tryAnalyzePredicateExpressionForSetIndex(
        const RPNBuilderTreeNode & arg, const BuildInfo & info, bool allow_relaxed_pruning);

    /// Appends to `group` the set atoms for its `IN` or `has` predicate using the materialized set
    /// and its analysis. Initial component and packed-tuple atoms take priority over additional
    /// deterministic transforms for the remaining key columns. Coverage is local to the group.
    /// `has_element_type` supplies the occupied element type of a `has` array, so every atom checks
    /// that conversion preserves its comparison semantics.
    void appendSetAtoms(
        const BuildInfo & info,
        const Columns & set_columns,
        const DataTypes & set_types,
        SetIndexAnalysisResult analysis,
        bool allow_relaxed_pruning,
        AtomGroup & group,
        const DataTypePtr & has_element_type = nullptr);

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

    /// Marks range atoms that cannot exclude tuple-contained NaNs as inexact and refreshes derived exactness.
    /// Requires the full primary-key types in key-column order.
    void relaxRangeAtomsForTupleNaNs(const DataTypes & key_types);

    /// Rebuilds the derived exactness condition after changing the RPN, without modifying shared copies.
    void updateExactnessCondition();

    /// Combines pruning and falsity through the supplied range evaluator.
    template <typename Evaluate>
    BoolMask checkWithExactness(const Evaluate & evaluate, BoolMask initial_mask) const;

    /// In every multi-atom group that stands directly under `FUNCTION_NOT` and has at least one
    /// exact atom, drops the relaxed atoms: a relaxed atom forces the group's `can_be_false` to
    /// `true`, which would disable pruning through the exact atoms of the group under `NOT`.
    void dropCoveredRelaxedAtomsFromNegatedGroups();

    /// Whether this element completes a predicate's atom group or is an independent logical operator.
    static bool isAtomGroupEnd(const RPN & rpn, size_t position);

    /// Returns `rpn` with the relaxed atoms dropped from every multi-atom group that contains
    /// both an exact atom and a relaxed one (with `only_negated_groups`, only from the groups
    /// standing directly under `FUNCTION_NOT`), or nothing when no group qualifies. Every atom
    /// of a group is a necessary condition of the same predicate leaf, and an exact atom is an
    /// exact index approximation of that leaf on its own, so the result describes the same
    /// predicate: the dropped atoms only added pruning (`can_be_true`) at the cost of an
    /// unreliable `can_be_false`.
    static std::optional<RPN> dropCoveredRelaxedAtoms(const RPN & rpn, bool only_negated_groups);

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

    struct SpaceFillingCurveArgument
    {
        String name;
        DataTypePtr type;
    };

    struct SpaceFillingCurveDescription
    {
        size_t key_column_pos{};
        String function_name;
        std::vector<SpaceFillingCurveArgument> arguments;
        SpaceFillingCurveType type{};
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

    /// Holds the value of the `analyze_index_with_multiple_key_columns_per_condition` setting.
    /// When false, atom extraction keeps at most one atom per predicate leaf: the candidate
    /// sources of `extractComparisonAtomsForKeyArgument` are consulted in priority order until one
    /// of them matches, and the set analysis builds only the direct set atom. Multi-atom groups
    /// then never form, but a single tuple-set atom can still constrain several key columns.
    bool multiple_key_columns_per_condition = true;

    /// Holds whether the key columns are sorted in reverse (ORDER BY ... DESC) or not.
    KeyOrder key_order;

    /// Stores an eligible condition with covered relaxed siblings removed, used only for falsity checks.
    /// It is null when the original condition suffices or no eligible derivative exists.
    std::shared_ptr<KeyCondition> exactness_condition;
};
}
