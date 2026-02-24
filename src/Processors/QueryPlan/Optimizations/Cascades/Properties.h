#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <base/types.h>
#include <vector>

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

    void dump(WriteBuffer & out) const;
    String dump() const;
};

struct ExpressionProperties
{
    SortDescription sorting;
    UInt64 sort_limit = 0;  /// Limit applied together with ORDER BY. Passed to SortingStep; 0 means no limit.
    DistributionDescription distribution;

    bool isSatisfiedBy(const ExpressionProperties & existing_properties) const;

    static bool isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing);
    static bool isDistributionSatisfiedBy(const DistributionDescription & required, const DistributionDescription & existing);

    void dump(WriteBuffer & out) const;
    String dump() const;
};

}
