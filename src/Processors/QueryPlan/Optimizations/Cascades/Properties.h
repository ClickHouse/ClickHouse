#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <base/types.h>
#include <vector>
#include <functional>
#include <boost/functional/hash.hpp>

namespace DB
{

/// Returns {max_node_count} — the full cluster.
/// Intermediate counts ({2, 4, 8, ...}) were explored previously but never
/// chosen on TPC-H. Still this might be revisited in the future.
inline std::vector<size_t> getCandidateNodeCounts(size_t max_node_count)
{
    if (max_node_count <= 1)
        return {};
    return {max_node_count};
}

/// A set of columns, but each column can also have multiple equivalent names derived from equality predicates
using DistributionColumns = std::vector<NameSet>;

struct DistributionDescription
{
    DistributionColumns columns;    /// Columns by which data is distributed. E.g. for shuffle exchange or partitioned table scan.
    /// Type each key is cast to before hashing (parallel to `columns`; empty when the raw
    /// types agree). Aligns buckets across both sides of a shuffle join.
    Names hash_type_names;
    bool is_replicated = false;     /// All data is replicated to all nodes, so any distribution is satisfied. E.g. for small tables that are broadcasted to all nodes.
    size_t node_count = 1;          /// Number of nodes among which data is distributed. E.g. for shuffle exchange of partitioned read.

    bool operator==(const DistributionDescription & other) const = default;

    void dump(WriteBuffer & out) const;
    String dump() const;
};

struct ExpressionProperties
{
    SortDescription sorting;
    UInt64 sort_limit = 0;  /// Limit applied together with ORDER BY. Passed to SortingStep; 0 means no limit.
    DistributionDescription distribution;

    bool operator==(const ExpressionProperties & other) const = default;

    bool isSatisfiedBy(const ExpressionProperties & existing_properties) const;

    static bool isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing);
    static bool isDistributionSatisfiedBy(const DistributionDescription & required, const DistributionDescription & existing);

    void dump(WriteBuffer & out) const;
    String dump() const;
};

struct ExpressionPropertiesHash
{
    size_t operator()(const ExpressionProperties & props) const
    {
        size_t h = std::hash<size_t>()(props.distribution.node_count);
        boost::hash_combine(h, props.distribution.is_replicated);
        boost::hash_combine(h, props.sort_limit);
        for (const auto & col_set : props.distribution.columns)
        {
            /// Equal sets must hash equally regardless of insertion order.
            size_t set_hash = 0;
            for (const auto & name : col_set)
                set_hash += std::hash<String>()(name);
            boost::hash_combine(h, set_hash);
        }
        for (const auto & type_name : props.distribution.hash_type_names)
            boost::hash_combine(h, type_name);
        for (const auto & col : props.sorting)
            boost::hash_combine(h, col.column_name);
        return h;
    }
};

/// Composite key for tracking which (rule, properties) pairs have been applied.
struct RulePropertiesKey
{
    const void * rule_ptr;  /// Address of the rule (stable for the optimizer's lifetime)
    ExpressionProperties properties;

    bool operator==(const RulePropertiesKey & other) const = default;
};

struct RulePropertiesKeyHash
{
    size_t operator()(const RulePropertiesKey & key) const
    {
        size_t h = std::hash<const void *>()(key.rule_ptr);
        boost::hash_combine(h, ExpressionPropertiesHash()(key.properties));
        return h;
    }
};

}
