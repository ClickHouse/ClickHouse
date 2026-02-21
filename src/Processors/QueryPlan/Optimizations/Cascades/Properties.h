#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>

namespace DB
{

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
    DistributionDescription distribution;

    bool isSatisfiedBy(const ExpressionProperties & existing_properties) const;

    static bool isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing);
    static bool isDistributionSatisfiedBy(const DistributionDescription & required, const DistributionDescription & existing);

    void dump(WriteBuffer & out) const;
    String dump() const;
};

}
