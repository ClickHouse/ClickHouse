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
    /// TODO: Support min max
    /// Field min_value, max_value;
    UInt64 num_distinct_values = 0;
    /// Average uncompressed size of one value; 0 means unknown.
    Float64 avg_bytes = 0;
};

struct RelationProfile
{
    UInt64 rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};
};

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Estimates the selectivity of a condition and cardinality of columns.
/// Deliberately holds no query context: instances are cached and shared across queries
/// (`SelectivityEstimatorCache`), so callers pass their own context per call.
class ConditionSelectivityEstimator
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
    ConditionSelectivityEstimator() = default;

    RelationProfile estimateRelationProfile(const ContextPtr & context, const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const;
    RelationProfile estimateRelationProfile(const ContextPtr & context, const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const std::vector<RPNBuilderTreeNode> & nodes) const;
    RelationProfile estimateRelationProfile() const;

    /// Approximate memory usage, for cache-weight accounting (`SelectivityEstimatorCache`).
    size_t memoryUsageBytes() const;

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

    /// Selectivity of `column IN (set)` derived from the size of the set rather than from its contents:
    /// the share of rows inside the set's bounding range, capped by the share of distinct values the set
    /// can possibly cover. Costs one pass for the bounds and a single statistics probe, where turning the
    /// set into ranges costs a `Field` per element, a sort and one probe per element.
    Selectivity estimateSelectivityFromSetSize(
        const StorageMetadataPtr & metadata, const String & column_name, const IColumn & set_elements, bool negative) const;
    UInt64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr Float64 default_cond_range_factor = 0.33;
    static constexpr Float64 default_cond_equal_factor = 0.01;
    static constexpr Float64 default_unknown_cond_factor = 0.33;
    static constexpr Float64 default_like_factor = 0.1;
    static constexpr Float64 default_cardinality_ratio = 0.1;

    UInt64 total_rows = 0;
    ColumnEstimators column_estimators;
};

/// Consumers only estimate; the mutable pointer exists only inside the builder and the cache.
using ConditionSelectivityEstimatorPtr = std::shared_ptr<const ConditionSelectivityEstimator>;

class ConditionSelectivityEstimatorBuilder
{
public:
    ConditionSelectivityEstimatorBuilder();
    void addStatistics(const String & column_name, const ColumnStatisticsPtr & column_stats);
    void incrementRowCount(UInt64 rows);
    std::shared_ptr<ConditionSelectivityEstimator> getEstimator() const;

private:
    bool has_data = false;
    std::shared_ptr<ConditionSelectivityEstimator> estimator;
};

}
