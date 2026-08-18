#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <fmt/ranges.h>

namespace DB
{

bool ExpressionProperties::isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing)
{
    /// Required sorting is satisfied by existing sorting if required is the prefix of existing
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

    /// The partition hash chains the key columns in order and may cast them first, so the
    /// match must be positional and the hash types must agree: data shuffled by `(b, a)`
    /// or hashed at a different type does not colocate with data shuffled by `(a, b)`.
    if (required.hash_type_names != existing.hash_type_names)
        return false;

    for (size_t i = 0; i < required.columns.size(); ++i)
    {
        const auto & required_column = required.columns[i];
        const auto & existing_column = existing.columns[i];
        bool found = false;
        for (const auto & equivalent_column : required_column)
        {
            if (existing_column.contains(equivalent_column))
            {
                found = true;
                break;
            }
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
    out << dumpSortDescription(sorting);
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
