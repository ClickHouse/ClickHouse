#pragma once

#include <Storages/Statistics/Statistics.h>

#include <Core/Field.h>
#include <Core/PlainRanges.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class RPNBuilderTreeNode;
class RPNBuilderFunctionTreeNode;

struct ColumnStats
{
    /// TODO: Support min max
    /// Field min_value, max_value;
    UInt64 num_distinct_values = 0;
};

struct RelationProfile
{
    UInt64 rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};
};

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
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
        /// For per-column predicates, keep how the predicate behaves on rows where the column is NULL.
        /// This lets same-column combinations with IS NULL / IS NOT NULL avoid treating NULL rows as
        /// independent from the predicate they came from.
        Float64 input_null_sel;
        Float64 true_on_null_sel;
        Float64 null_on_null_sel;

        Selectivity()
            : true_sel(0)
            , null_sel(0)
            , input_null_sel(0)
            , true_on_null_sel(0)
            , null_on_null_sel(0)
        {
        }
        explicit Selectivity(Float64 true_sel_)
            : true_sel(true_sel_)
            , null_sel(0)
            , input_null_sel(0)
            , true_on_null_sel(0)
            , null_on_null_sel(0)
        {
        }
        /// Ordinary predicates over Nullable columns evaluate to NULL exactly on the column's NULL rows.
        Selectivity(Float64 true_sel_, Float64 null_sel_)
            : true_sel(true_sel_)
            , null_sel(null_sel_)
            , input_null_sel(null_sel_)
            , true_on_null_sel(0)
            , null_on_null_sel(null_sel_)
        {
        }
        Selectivity(Float64 true_sel_, Float64 null_sel_, Float64 input_null_sel_, Float64 true_on_null_sel_, Float64 null_on_null_sel_)
            : true_sel(true_sel_)
            , null_sel(null_sel_)
            , input_null_sel(input_null_sel_)
            , true_on_null_sel(true_on_null_sel_)
            , null_on_null_sel(null_on_null_sel_)
        {
        }

        static Selectivity isNull(Float64 input_null_sel_);
        static Selectivity isNotNull(Float64 input_null_sel_);

        Selectivity applyNot() const;
        Selectivity applyOr(const Selectivity & other) const;
        Selectivity applyAnd(const Selectivity & other) const;
        Selectivity applyOrSameColumn(const Selectivity & other) const;
        Selectivity applyAndSameColumn(const Selectivity & other) const;
    };

    friend class ConditionSelectivityEstimatorBuilder;
public:
    explicit ConditionSelectivityEstimator(ContextPtr context_) : WithContext(context_) {}

    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const std::vector<RPNBuilderTreeNode> & nodes) const;
    RelationProfile estimateRelationProfile() const;

    bool isStale(const std::vector<DataPartPtr> & data_parts) const;

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
        using ColumnSelectivities = std::unordered_map<String, Selectivity>;
        /// column in range (a, b) ...
        ColumnRanges column_ranges;
        /// column predicates that cannot be represented as ranges but still share the column's NULL domain.
        ColumnSelectivities column_selectivities;
        /// column not in range (a, b) ...
        /// we use 'not ranges' to estimate condition a != 1 and a != 2 better.
        ColumnRanges column_not_ranges;
        /// columns checked with IS NULL predicate
        std::unordered_set<String> null_check_columns;
        /// columns checked with IS NOT NULL predicate
        std::unordered_set<String> not_null_check_columns;
        bool finalized = false;
        Selectivity selectivity;

        bool tryToMergeClauses(RPNElement & lhs, RPNElement & rhs);
        void finalize(const ColumnEstimators & column_estimators_, const StorageMetadataPtr & metadata);
    };
    using AtomMap = std::unordered_map<std::string, void(*)(RPNElement & out, const String & column, const Field & value)>;
    static const AtomMap atom_map;

    UInt64 getTotalRows() const { return total_rows; }

private:
    friend class ColumnStatistics;

    struct ColumnEstimator
    {
        ColumnStatisticsPtr stats;

        Selectivity estimateRanges(const PlainRanges & ranges) const;
        UInt64 estimateCardinality() const;
    };

    RelationProfile estimateRelationProfileImpl(std::vector<RPNElement> & rpn, const StorageMetadataPtr & metadata) const;
    bool extractAtomFromTree(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node, RPNElement & out) const;
    bool
    tryExtractStringPredicateAtom(const StorageMetadataPtr & metadata, const RPNBuilderFunctionTreeNode & func, RPNElement & out) const;
    bool
    tryExtractLikePredicateAtom(const StorageMetadataPtr & metadata, const RPNBuilderFunctionTreeNode & func, RPNElement & out) const;
    /// Share of NULL rows in a column: statistics-based when available, a default heuristic otherwise.
    Float64
    estimateColumnNullShare(const StorageMetadataPtr & metadata, const DataTypePtr & column_type, const String & column_name) const;
    UInt64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr Float64 default_cond_range_factor = 0.33;
    static constexpr Float64 default_cond_equal_factor = 0.01;
    static constexpr Float64 default_unknown_cond_factor = 0.33;
    static constexpr Float64 default_like_factor = 0.1;
    static constexpr Float64 default_cardinality_ratio = 0.1;

    UInt64 total_rows = 0;
    ColumnEstimators column_estimators;
    Strings parts_names;
};

using ConditionSelectivityEstimatorPtr = std::shared_ptr<ConditionSelectivityEstimator>;

class ConditionSelectivityEstimatorBuilder
{
public:
    explicit ConditionSelectivityEstimatorBuilder(ContextPtr context_);
    void addStatistics(const String & column_name, const ColumnStatisticsPtr & column_stats);
    void incrementRowCount(UInt64 rows);
    void markDataPart(const DataPartPtr & data_part);
    ConditionSelectivityEstimatorPtr getEstimator() const;

private:
    bool has_data = false;
    ConditionSelectivityEstimatorPtr estimator;
};

}
