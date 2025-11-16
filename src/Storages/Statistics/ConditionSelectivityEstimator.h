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

    friend class ConditionSelectivityEstimatorBuilder;
public:
    explicit ConditionSelectivityEstimator(ContextPtr context_) : WithContext(context_) {}

    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * filter, const ActionsDAG::Node * prewhere) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const ActionsDAG::Node * node) const;
    RelationProfile estimateRelationProfile(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node) const;
    RelationProfile estimateRelationProfile() const;

    bool isStale(const std::vector<DataPartPtr> & data_parts) const;

    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN_RANGE,
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
        bool finalized = false;
        Float64 selectivity;

        bool tryToMergeClauses(RPNElement & lhs, RPNElement & rhs);
        void finalize(const ColumnEstimators & column_estimators_);
    };
    using AtomMap = std::unordered_map<std::string, void(*)(RPNElement & out, const String & column, const Field & value)>;
    static const AtomMap atom_map;
private:
    friend class ColumnStatistics;

    struct ColumnEstimator
    {
        ColumnStatisticsPtr stats;

        Float64 estimateRanges(const PlainRanges & ranges) const;
        UInt64 estimateCardinality() const;
    };

    RelationProfile estimateRelationProfileImpl(std::vector<RPNElement> & rpn) const;
    bool extractAtomFromTree(const StorageMetadataPtr & metadata, const RPNBuilderTreeNode & node, RPNElement & out) const;
    UInt64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr Float64 default_cond_range_factor = 0.5;
    static constexpr Float64 default_cond_equal_factor = 0.01;
    static constexpr Float64 default_unknown_cond_factor = 1;
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
    void addStatistics(ColumnStatisticsPtr column_stats);
    void incrementRowCount(UInt64 rows);
    void markDataPart(const DataPartPtr & data_part);
    ConditionSelectivityEstimatorPtr getEstimator() const;

private:
    bool has_data = false;
    ConditionSelectivityEstimatorPtr estimator;
};

}
