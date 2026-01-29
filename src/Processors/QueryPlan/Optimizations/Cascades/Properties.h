#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>

namespace DB
{

/// A set of columns, but each column can also have multiple equivalent names derived from equality predicates
using DistributionColumns = std::vector<NameSet>;

struct ExpressionProperties
{
    SortDescription sorting;
    DistributionColumns distribution_columns;

    bool isSatisfiedBy(const ExpressionProperties & existing_properties) const;

    static bool isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing);
    static bool isDistributionSatisfiedBy(const DistributionColumns & required, const DistributionColumns & existing);

    void dump(WriteBuffer & out) const;
    String dump() const;
};

}
