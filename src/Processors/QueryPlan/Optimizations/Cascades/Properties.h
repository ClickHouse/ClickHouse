#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <base/types.h>
#include <vector>
#include <functional>

namespace DB
{

/// Returns {2, 4, 8, ..., max_node_count} (powers of 2, capped at max).
/// Always includes max_node_count even if not a power of 2.
inline std::vector<size_t> getCandidateNodeCounts(size_t max_node_count)
{
    std::vector<size_t> result;
    for (size_t candidate = 2; candidate <= max_node_count; candidate *= 2)
        result.push_back(candidate);
    if (!result.empty() && result.back() != max_node_count)
        result.push_back(max_node_count);
    return result;
}

/// A set of columns, but each column can also have multiple equivalent names derived from equality predicates
using DistributionColumns = std::vector<NameSet>;

struct DistributionDescription
{
    DistributionColumns columns;    /// Columns by which data is distributed. E.g. for shuffle exchange or partitioned table scan.
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
        h ^= std::hash<bool>()(props.distribution.is_replicated) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<UInt64>()(props.sort_limit) + 0x9e3779b9 + (h << 6) + (h >> 2);
        for (const auto & col_set : props.distribution.columns)
            for (const auto & name : col_set)
                h ^= std::hash<String>()(name) + 0x9e3779b9 + (h << 6) + (h >> 2);
        for (const auto & col : props.sorting)
            h ^= std::hash<String>()(col.column_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
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
        h ^= ExpressionPropertiesHash()(key.properties) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

}
