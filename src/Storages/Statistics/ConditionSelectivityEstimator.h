#pragma once

#include <Storages/Statistics/Statistics.h>

#include <Core/Field.h>
#include <Core/PlainRanges.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class RPNBuilderTreeNode;

struct ColumnStats
{
    UInt64 num_distinct_values = 0;
    /// NDV of the column in the whole relation it comes from, before any filter; zero when unknown.
    /// Two filtered sets of the column's values overlap in proportion to their shares of it.
    UInt64 domain_distinct_values = 0;
    /// Rows of the table the column is read from, and the rows of it that the read selects; zero when unknown.
    UInt64 table_rows = 0;
    UInt64 table_selected_rows = 0;
    /// Average uncompressed size of one value; 0 means unknown.
    Float64 avg_bytes = 0;
    /// Value range, as values of `value_type`; unset when unknown. It comes from `basic`/`minmax` statistics and
    /// a monotonic function can map it. A user must check the type: a `JOIN USING` can change the type of the
    /// column and keep the range. A step that can add values outside the range clears it.
    std::optional<Field> min_value = {};
    std::optional<Field> max_value = {};
    DataTypePtr value_type = nullptr;
    /// Fraction of NULL values; unset when unknown.
    std::optional<Float64> null_fraction = {};
    /// Most distinct values the column can have, proven by a filter (`x = c`, `x IN (c1, ...)`) and kept
    /// by what follows it; zero when nothing proves a limit. Unlike `num_distinct_values` it never
    /// comes from a sketch, so the row bounds may use it.
    UInt64 max_distinct_values = 0;
    /// Rows of the column that its statistics do not describe (parts without statistics of the column); each can add
    /// a value that `num_distinct_values` does not count.
    UInt64 rows_without_statistics = 0;
    /// Every value of the column is on at most one row: the column is the only key of an aggregation. A join on it
    /// matches each row of the other side at most once. Whatever can repeat a row (a join, a union) drops the flag.
    bool unique = false;

    /// Whether the rows without statistics could add as many values as `num_distinct_values` counts, or more, within
    /// the proven limit: a group count, a join or a filter that uses the NDV is then not precise.
    bool hasImpreciseDistinctValues() const
    {
        if (rows_without_statistics == 0 || num_distinct_values == 0)
            return false;
        UInt64 new_values = rows_without_statistics;
        if (max_distinct_values != 0)
            new_values = std::min(new_values, max_distinct_values - std::min(max_distinct_values, num_distinct_values));
        return new_values >= num_distinct_values;
    }

    void clearValueRange()
    {
        min_value.reset();
        max_value.reset();
        value_type = nullptr;
    }
};

struct RelationProfile
{
    UInt64 rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};
    /// A predicate the statistics could not estimate took the default selectivity, so `rows` is a guess.
    bool estimate_from_defaults = false;
};

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Estimates the selectivity of a condition and cardinality of columns.
class ConditionSelectivityEstimator : public WithContext
{
    struct ColumnEstimator;
    using ColumnEstimators = std::unordered_map<String, ColumnEstimator>;

    /// Selectivity of a SQL boolean predicate under three-valued logic (TRUE / NULL / FALSE).
    /// `true_sel` is the fraction of rows where the predicate is TRUE (the usual "selectivity").
    /// `null_sel` is the fraction of rows where the predicate is NULL (input column is NULL).
    /// The FALSE fraction is implicitly `1 - true_sel - null_sel`.
    struct Selectivity
    {
        Float64 true_sel;
        Float64 null_sel;

        Selectivity() : true_sel(0), null_sel(0) {}
        explicit Selectivity(Float64 true_sel_) : true_sel(true_sel_), null_sel(0) {}
        Selectivity(Float64 true_sel_, Float64 null_sel_) : true_sel(true_sel_), null_sel(null_sel_) {}
        Selectivity applyNot() const;
        Selectivity applyOr(const Selectivity & other) const;
        Selectivity applyAnd(const Selectivity & other) const;
    };

    friend class ConditionSelectivityEstimatorBuilder;
public:
    explicit ConditionSelectivityEstimator(ContextPtr context_) : WithContext(context_) {}

    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const;
    /// `row_fraction` scales the rows the conditions keep: the share of the table an index analysis has
    /// already selected for conditions left out of `nodes`.
    RelationProfile estimateRelationProfile(
        const StorageMetadataPtr & metadata, const std::vector<RPNBuilderTreeNode> & nodes, Float64 row_fraction = 1.0) const;
    RelationProfile estimateRelationProfile() const;

    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN_RANGE,
            FUNCTION_IS_NULL,
            FUNCTION_IS_NOT_NULL,
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        Function function = FUNCTION_UNKNOWN;
        using ColumnRanges = std::unordered_map<String, PlainRanges>;
        /// column in range (a, b) ...
        ColumnRanges column_ranges;
        /// column not in range (a, b) ...
        /// we use 'not ranges' to estimate condition a != 1 and a != 2 better.
        ColumnRanges column_not_ranges;
        /// columns checked with IS NULL predicate
        std::unordered_set<String> null_check_columns;
        /// columns checked with IS NOT NULL predicate
        std::unordered_set<String> not_null_check_columns;
        bool finalized = false;
        /// The default selectivity stood in for this element or for an atom it absorbed.
        bool used_default = false;
        Selectivity selectivity;
        /// Selectivity of the atoms that were absorbed by a conjunctive merge without contributing
        /// any range - currently only `FUNCTION_UNKNOWN`. Merging carries ranges across, so such an
        /// atom has nothing to add to the merged clause, but it still has to be accounted for.
        /// Keeping it here instead of finalizing the clause lets the ranges of a later conjunct on
        /// the same column still merge with the earlier ones. `finalize` applies it at the end.
        /// Only meaningful under `FUNCTION_AND`: `P(a OR unknown)` is not `P(a) * f`.
        Selectivity absorbed_and_selectivity{1.0, 0.0};

        bool hasAbsorbed() const { return absorbed_and_selectivity.true_sel != 1.0 || absorbed_and_selectivity.null_sel != 0.0; }

        /// Carries no ranges and no null checks, so it contributes a bare number and nothing a merge
        /// could represent: an unknown atom, a `LIKE` estimated by a default, or a clause already
        /// reduced to a selectivity. Under `AND` such a factor is absorbed rather than forcing both
        /// sides to be finalized, which is what keeps the ranges around it mergeable.
        ///
        /// `ALWAYS_TRUE` and `ALWAYS_FALSE` are deliberately excluded although they carry no ranges
        /// either. They are read back as a `function` - the `AND`/`OR` folding drops or propagates a
        /// constant operand by its tag, and `NOT` flips the tag - while `finalize` only ever sets
        /// `selectivity`. Treating them as a factor would leave the tag saying the opposite of the
        /// number and the folding would act on the tag.
        bool isConstantFactor() const
        {
            if (function == ALWAYS_TRUE || function == ALWAYS_FALSE)
                return false;

            return column_ranges.empty() && column_not_ranges.empty()
                && null_check_columns.empty() && not_null_check_columns.empty();
        }

        bool tryToMergeClauses(RPNElement & lhs, RPNElement & rhs);
        void finalize(const ColumnEstimators & column_estimators_, const StorageMetadataPtr & metadata);
    };
    using AtomMap = std::unordered_map<std::string, void(*)(RPNElement & out, const String & column, const Field & value)>;
    static const AtomMap atom_map;

    UInt64 getTotalRows() const { return total_rows; }
    /// Rows without statistics (parts without statistics of a column) are estimated from the other rows, as from a
    /// sample: they can change the row estimate by up to their number, and add up to their number of distinct values
    /// to a column. Records them in the columns of `profile`, and returns whether they could double its rows.
    bool recordRowsWithoutStatistics(RelationProfile & profile) const;

private:
    friend class ColumnStatistics;

    struct ColumnEstimator
    {
        ColumnStatisticsPtr stats;

        /// `used_default` is set when a range took a default factor because the statistics could not estimate it.
        Selectivity estimateRanges(const PlainRanges & ranges, bool & used_default) const;
        UInt64 estimateCardinality() const;
    };

    RelationProfile estimateRelationProfileImpl(std::vector<RPNElement> & rpn, const StorageMetadataPtr & metadata, Float64 row_fraction = 1.0) const;
    bool extractAtomFromTree(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node, RPNElement & out) const;

    /// Selectivity of `column IN (set)` derived from the size of the set rather than from its contents:
    /// the share of rows inside the set's bounding range, capped by the share of distinct values the set
    /// can possibly cover. Costs one pass for the bounds and a single statistics probe, where turning the
    /// set into ranges costs a `Field` per element, a sort and one probe per element.
    Selectivity estimateSelectivityFromSetSize(
        const StorageMetadataPtr & metadata, const String & column_name, const IColumn & set_elements,
        const DataTypePtr & set_type, bool negative, bool & used_default) const;
    UInt64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    /// Selectivity of an equality with any of `num_values` values of `type` when no statistics can estimate it.
    /// Only a `Bool` counts the values: the default for other types is flat.
    static Float64 getEqualFactor(const DataTypePtr & type, size_t num_values = 1);

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr Float64 default_cond_range_factor = 0.33;
    static constexpr Float64 default_cond_equal_factor = 0.01;
    static constexpr Float64 default_bool_equal_factor = 0.5;
    static constexpr Float64 default_unknown_cond_factor = 0.33;
    static constexpr Float64 default_like_factor = 0.1;
    static constexpr Float64 default_cardinality_ratio = 0.1;

    UInt64 total_rows = 0;
    /// The rows of the parts that lack the statistics of some column.
    UInt64 rows_without_statistics = 0;
    ColumnEstimators column_estimators;
};

using ConditionSelectivityEstimatorPtr = std::shared_ptr<ConditionSelectivityEstimator>;

/// `equals(x, x)` or `isNotDistinctFrom(x, x)` of one node (aliases are looked through) whose type makes it
/// true on every row: no NaN or composite values, and no NULL for `equals`.
bool isAlwaysTrueEquality(const ActionsDAG::Node & node);

/// Builds an estimator for a set of data parts by merging their statistics column by column.
/// The statistics passed in are not modified, so they may come from the statistics cache.
class ConditionSelectivityEstimatorBuilder
{
public:
    explicit ConditionSelectivityEstimatorBuilder(ContextPtr context_);
    /// Starts the next part; the statistics added after it are of this part.
    void addPart(UInt64 rows);
    void addStatistics(const String & column_name, const ColumnStatisticsPtr & column_stats);
    ConditionSelectivityEstimatorPtr getEstimator() const;

private:
    bool has_data = false;
    ConditionSelectivityEstimatorPtr estimator;
    /// The rows of each part and the number of columns it has statistics of.
    std::vector<std::pair<UInt64, size_t>> parts_columns;
};

}
