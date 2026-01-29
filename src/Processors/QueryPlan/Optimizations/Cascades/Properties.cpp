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

bool ExpressionProperties::isDistributionSatisfiedBy(const DistributionColumns & required, const DistributionColumns & existing)
{
    /// Required distribution is satisfied if existing has all the columns from required
    if (required.size() > existing.size())
        return false;
    for (const auto & required_column : required)
    {
        bool found = false;
        for (const auto & equivalent_column : required_column)
        {
            for (const auto & existing_column : existing)
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
        isDistributionSatisfiedBy(distribution_columns, existing_properties.distribution_columns);
}

void  ExpressionProperties::dump(WriteBuffer & out) const
{
    out << "{[";
    dumpSortDescription(sorting, out);
    out << "], {";
    out << fmt::format("{}", fmt::join(distribution_columns, ","));
    out << "}}";
}

String  ExpressionProperties::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
