#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <fmt/ranges.h>
#include <algorithm>
#include <numeric>

namespace DB
{

void ExpressionProperties::setDisjointStreams(DistributionColumns columns)
{
    chassert(!columns.empty());
    /// Order the sets by their full sorted name lists: comparing only one name per set would
    /// leave sets that share it in an unspecified order, and the same disjointness written
    /// in a different set order would then compare and hash unequal. The sort permutes
    /// indexes over precomputed keys; sorting the sets directly would let the comparator
    /// read a set that `std::sort` has moved out.
    std::vector<Names> sort_keys(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        chassert(!columns[i].empty());
        sort_keys[i].assign(columns[i].begin(), columns[i].end());
        std::sort(sort_keys[i].begin(), sort_keys[i].end());
    }
    std::vector<size_t> order(columns.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(),
        [&](size_t left, size_t right) { return sort_keys[left] < sort_keys[right]; });

    stream_layout = StreamLayout::Disjoint;
    stream_disjoint_columns.clear();
    stream_disjoint_columns.reserve(columns.size());
    for (size_t i : order)
        stream_disjoint_columns.push_back(std::move(columns[i]));
}

bool ExpressionProperties::isStreamLayoutSatisfiedBy(const ExpressionProperties & required, const ExpressionProperties & existing)
{
    if (required.stream_layout == StreamLayout::Unknown)
        return true;
    /// One stream trivially keeps every group of equal rows whole.
    if (existing.stream_layout == StreamLayout::Single)
        return true;
    if (required.stream_layout == StreamLayout::Single || existing.stream_layout != StreamLayout::Disjoint)
        return false;

    /// Streams disjoint on the existing columns satisfy a disjointness requirement when every
    /// existing column set matches one of the required sets: rows equal on the required
    /// columns are then equal on the existing ones, so they share a stream.
    for (const auto & existing_column : existing.stream_disjoint_columns)
    {
        bool found = false;
        for (const auto & required_column : required.stream_disjoint_columns)
        {
            for (const auto & name : existing_column)
            {
                if (required_column.contains(name))
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
        isDistributionSatisfiedBy(distribution, existing_properties.distribution) &&
        isStreamLayoutSatisfiedBy(*this, existing_properties);
}

void  ExpressionProperties::dump(WriteBuffer & out) const
{
    out << "{[";
    out << dumpSortDescription(sorting);
    out << "], {";
    out << fmt::format("{} nodes, {}, {}", distribution.node_count, distribution.is_replicated ? "replicated" : "not replicated", fmt::join(distribution.columns, ","));
    if (stream_layout == StreamLayout::Single)
        out << ", single stream";
    else if (stream_layout == StreamLayout::Disjoint)
        out << fmt::format(", streams disjoint on {}", fmt::join(stream_disjoint_columns, ","));
    out << "}}";
}

String  ExpressionProperties::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
