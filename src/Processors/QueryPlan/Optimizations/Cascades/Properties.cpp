#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <fmt/ranges.h>

namespace DB
{

bool ExpressionProperties::isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing)
{
    /// Required sorting is satifsied by existing sorting if required is the prefix of existing
    if (required.size() > existing.size())
        return false;
    for (size_t i = 0; i < required.size(); ++i)
    {
        if (required[i] != existing[i])
            return false;
    }
    return true;
}

bool ExpressionProperties::isDistributionSatisfiedBy(const DistributionDescription & required, const DistributionDescription & existing)
{
    if (required.node_count != existing.node_count)
        return false;

    if (required.is_replicated != existing.is_replicated)
        return false;

    /// Empty required columns means "any distribution is fine" - just match node_count
    /// and replication. Data shuffled by specific columns still satisfies a requirement
    /// that doesn't care about column distribution.
    if (required.columns.empty())
        return true;

    if (required.columns.size() != existing.columns.size())
        return false;

    for (const auto & required_column : required.columns)
    {
        bool found = false;
        for (const auto & equivalent_column : required_column)
        {
            for (const auto & existing_column : existing.columns)
            {
                if (existing_column.contains(equivalent_column))
                {
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
        if (!found)
            return false;
    }
    return true;
}

bool ExpressionProperties::isSatisfiedBy(const ExpressionProperties & existing_properties) const
{
    return isSortingSatisfiedBy(sorting, existing_properties.sorting) &&
        isDistributionSatisfiedBy(distribution, existing_properties.distribution);
}

void  ExpressionProperties::dump(WriteBuffer & out) const
{
    out << "{[";
    dumpSortDescription(sorting, out);
    out << "], {";
    out << fmt::format("{} nodes, {}, {}", distribution.node_count, distribution.is_replicated ? "replicated" : "not replicated", fmt::join(distribution.columns, ","));
    out << "}}";
}

String  ExpressionProperties::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
