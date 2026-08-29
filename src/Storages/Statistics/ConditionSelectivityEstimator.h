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

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct RangesInDataParts;

/// Estimates the selectivity of a condition and cardinality of columns.
class ConditionSelectivityEstimator : public WithContext
{
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

    struct ColumnEstimator
    {
        ColumnStatisticsPtr stats;

        Selectivity estimateRanges(const PlainRanges & ranges) const;
        UInt64 estimateCardinality() const;
    };
    using ColumnEstimators = std::unordered_map<String, ColumnEstimator>;

    /// Merged statistics of one ordered part sequence, read-only during estimation, so one instance
    /// can back estimators bound to different query contexts. Immutable once published: the merge
    /// mutates the first part's statistics object in place, so merging into it again double-counts.
    struct Payload
    {
        UInt64 total_rows = 0;
        ColumnEstimators column_estimators;
        Strings parts_names;
    };

    friend class ConditionSelectivityEstimatorBuilder;
public:
    using PayloadPtr = std::shared_ptr<const Payload>;

    ConditionSelectivityEstimator(PayloadPtr payload_, ContextPtr context_);

    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const std::vector<RPNBuilderTreeNode> & nodes) const;
    RelationProfile estimateRelationProfile() const;

    /// Return true if the estimator was built from a different ordered sequence of data parts.
    bool isStale(const std::vector<DataPartPtr> & data_parts) const;
    /// Perform the same check against an analyzed query part set. Mark ranges are intentionally
    /// ignored because the estimator contains whole-part statistics.
    bool isStale(const RangesInDataParts & parts) const;

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

    UInt64 getTotalRows() const { return payload->total_rows; }

private:
    friend class ColumnStatistics;

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

    PayloadPtr payload;
};

using ConditionSelectivityEstimatorPtr = std::shared_ptr<ConditionSelectivityEstimator>;
using ConditionSelectivityPayloadPtr = ConditionSelectivityEstimator::PayloadPtr;

class ConditionSelectivityEstimatorBuilder
{
public:
    explicit ConditionSelectivityEstimatorBuilder(ContextPtr context_);
    void addStatistics(const String & column_name, const ColumnStatisticsPtr & column_stats);
    void incrementRowCount(UInt64 rows);
    void markDataPart(const DataPartPtr & data_part);
    ConditionSelectivityEstimatorPtr getEstimator() const;
    /// The merged statistics alone, for callers that reuse them across estimators. Returns null
    /// when no part contributed statistics, matching `getEstimator`.
    ConditionSelectivityPayloadPtr getPayload() const;

private:
    bool has_data = false;
    ContextPtr context;
    std::shared_ptr<ConditionSelectivityEstimator::Payload> payload;
};

}
