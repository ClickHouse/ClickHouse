#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/defines.h>
#include <Common/VectorWithMemoryTracking.h>

#include <span>
#include <utility>

namespace DB
{

/// Children of source columns grouped by key, for example the columns of every `JSON` path in the source `JSON` columns.
/// It owns its storage: every group is a span over one buffer of this object.
/// The source columns must outlive this object, and the spans must not outlive it. Built by `groupColumns`.
class GroupedColumns
{
public:
    /// The number of groups.
    [[nodiscard]] size_t size() const { return group_offsets.size() - 1; }

    /// The columns of `group`, in the order of the source columns.
    std::span<const IColumn * const> operator[](size_t group) const &
    {
        chassert(group < size());
        return std::span<const IColumn * const>(
            columns.data() + group_offsets[group], group_offsets[group + 1] - group_offsets[group]);
    }

    std::span<const IColumn * const> operator[](size_t) const && = delete;

private:
    explicit GroupedColumns(size_t num_groups)
        : group_offsets(num_groups + 1, 0)
    {
    }

    template <typename Children>
    friend GroupedColumns groupColumns(std::span<const IColumn * const> source_columns, size_t num_groups, Children && children);

    /// The columns of group `i` are in [group_offsets[i], group_offsets[i + 1]).
    VectorWithMemoryTracking<const IColumn *> columns;
    VectorWithMemoryTracking<size_t> group_offsets;
};

/// Groups the children of the source columns by key in one pass over the children that exist.
/// `children(source_column, emit)` must call `emit(group, child)` for every child of `source_column` that belongs to a
/// group, with `group` < `num_groups` and non-null `child`. A source column may have children in any number of groups.
/// Every group keeps the order of the source columns.
///
/// Use it when the children are selected by key (e.g. by name in a hash map) for many keys: looking up every key in every
/// source costs keys * sources lookups, mostly misses when every source has only a few of the keys, while grouping costs
/// one lookup per existing child. The groups are materialized, so traversing them doesn't repeat the lookups.
template <typename Children>
GroupedColumns groupColumns(std::span<const IColumn * const> source_columns, size_t num_groups, Children && children)
{
    GroupedColumns result(num_groups);
    if (num_groups == 0)
        return result;

    /// Collect the children with their groups in one pass, so that `children` (usually a lookup per child) runs once
    /// per child, and then put them into the groups with a stable counting sort.
    VectorWithMemoryTracking<std::pair<size_t, const IColumn *>> children_with_groups;
    /// Usually every group has a child in at least one source column.
    children_with_groups.reserve(num_groups);
    for (const auto & source_column : source_columns)
    {
        children(
            source_column,
            [&](size_t group, const IColumn * child)
            {
                chassert(group < num_groups);
                chassert(child);
                children_with_groups.emplace_back(group, child);
            });
    }

    /// Count the children of every group, then turn the counts into the ends of the groups.
    auto & offsets = result.group_offsets;
    for (const auto & [group, _] : children_with_groups)
        ++offsets[group];
    for (size_t group = 1; group != num_groups; ++group)
        offsets[group] += offsets[group - 1];
    offsets[num_groups] = children_with_groups.size();

    /// Fill backwards, so that every group keeps the order of the source columns and every offset ends at the start of its group.
    result.columns.resize(children_with_groups.size());
    for (auto it = children_with_groups.rbegin(); it != children_with_groups.rend(); ++it)
        result.columns[--offsets[it->first]] = it->second;

    return result;
}
}
