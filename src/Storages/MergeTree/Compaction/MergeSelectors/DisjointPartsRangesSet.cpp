#include <Storages/MergeTree/Compaction/MergeSelectors/DisjointPartsRangesSet.h>

namespace DB
{

bool DisjointPartsRangesSet::BoundariesComparator::operator()(const PartsRangeBoundaries & lhs, const PartsRangeBoundaries & rhs) const noexcept
{
    return lhs.range_begin < rhs.range_begin;
}

bool DisjointPartsRangesSet::BoundariesComparator::operator()(const PartsRangeBoundaries & lhs, const PartsIterator & rhs) const noexcept
{
    return lhs.range_begin < rhs;
}

bool DisjointPartsRangesSet::BoundariesComparator::operator()(const PartsIterator & lhs, const PartsRangeBoundaries & rhs) const noexcept
{
    return lhs < rhs.range_begin;
}

bool DisjointPartsRangesSet::isDisjoint(const PartsRangeBoundaries & lhs, const PartsRangeBoundaries & rhs)
{
    return lhs.range_end < rhs.range_begin || rhs.range_end < lhs.range_begin;
}

bool DisjointPartsRangesSet::isDisjoint(const PartsRangeBoundaries & boundaries, const SortedPartsRanges & sorted_ranges)
{
    auto it = sorted_ranges.upper_bound(boundaries);

    if (it != sorted_ranges.end())
        if (!isDisjoint(boundaries, *it))
            return false;

    if (it != sorted_ranges.begin())
        if (!isDisjoint(boundaries, *std::prev(it)))
            return false;

    return true;
}

bool DisjointPartsRangesSet::isCovered(const PartsIterator & part_it, const SortedPartsRanges & sorted_ranges)
{
    auto it = sorted_ranges.upper_bound(part_it);

    if (it == sorted_ranges.begin())
        return true;

    const auto & [range_begin, range_end] = *std::prev(it);
    return range_begin <= part_it && part_it <= range_end;
}


bool DisjointPartsRangesSet::isCovered(RangesIterator range_it, PartsIterator part_it) const
{
    auto sorted_ranges_it = disjoint.find(range_it.base());
    if (sorted_ranges_it == disjoint.end())
        return false;

    const auto & sorted_ranges = sorted_ranges_it->second;
    return isCovered(part_it, sorted_ranges);
}

bool DisjointPartsRangesSet::addRangeIfPossible(RangesIterator range_it, PartsIterator range_begin, PartsIterator range_end)
{
    auto & sorted_ranges = disjoint[range_it.base()];
    PartsRangeBoundaries range_boundaries{std::move(range_begin), std::move(range_end)};

    if (!isDisjoint(range_boundaries, sorted_ranges))
        return false;

    sorted_ranges.insert(std::move(range_boundaries));
    return true;
}

}
