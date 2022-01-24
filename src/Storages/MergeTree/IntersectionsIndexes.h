#pragma once

#include <fmt/format.h>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A boundary of a segment (left or right)
struct PartToRead
{
    PartBlockRange range;
    struct PartAndProjectionNames
    {
        String part;
        String projection;
        bool operator<(const PartAndProjectionNames & rhs) const
        {
            if (part == rhs.part)
                return projection < rhs.projection;
            return part < rhs.part;
        }
        bool operator==(const PartAndProjectionNames & rhs) const
        {
            return part == rhs.part && projection == rhs.projection;
        }
    };

    PartAndProjectionNames name;

    bool operator==(const PartToRead & rhs) const
    {
        return range == rhs.range && name == rhs.name;
    }

    bool operator<(const PartToRead & rhs) const
    {
        /// We allow only consecutive non-intersecting ranges
        const bool intersection =
            (range.begin <= rhs.range.begin && rhs.range.begin < range.end) ||
            (rhs.range.begin <= range.begin && range.begin <= rhs.range.end);
        if (intersection)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got intersecting parts. First [{}, {}]. Second [{}, {}]",
                range.begin, range.end, rhs.range.begin, rhs.range.end);
        return range.begin < rhs.range.begin && range.end <= rhs.range.begin;
    }
};

/// MergeTreeDataPart is described as a segment (min block and max block)
/// During request handling we have to know how many intersection
/// current part has with already saved parts in our state.
struct PartSegments
{
    enum class IntersectionResult
    {
        NO_INTERSECTION,
        EXACTLY_ONE_INTERSECTION,
        REJECT
    };

    void addPart(PartToRead part) { segments.insert(std::move(part)); }

    IntersectionResult getIntersectionResult(PartToRead part)
    {
        bool intersected_before = false;
        for (const auto & segment: segments)
        {
            auto are_intersect = [](auto & x, auto & y)
            {
                /// <= is important here, because we are working with segments [a, b]
                if ((x.begin <= y.begin) && (y.begin <= x.end))
                    return true;
                if ((y.begin <= x.begin) && (x.begin <= y.end))
                    return true;
                return false;
            };

            if (are_intersect(segment.range, part.range))
            {
                /// We have two or possibly more intersections
                if (intersected_before)
                    return IntersectionResult::REJECT;

                /// We have intersection with part with different name
                /// or with different min or max block
                /// It could happens if we have merged part on one replica
                /// but not on another.
                if (segment != part)
                    return IntersectionResult::REJECT;

                /// We allow only the intersection with the same part as we have
                intersected_before = true;
            }
        }

        return intersected_before ? IntersectionResult::EXACTLY_ONE_INTERSECTION : IntersectionResult::NO_INTERSECTION;
    }

    using OrderedSegments = std::set<PartToRead>;
    OrderedSegments segments;
};

/// This is used only in parallel reading from replicas
/// This struct is an ordered set of half intervals and it is responsible for
/// giving an inversion of that intervals (e.g. [a, b) => {[-inf, a), [b, +inf)})
/// or giving an intersection of two sets of intervals
/// This is needed, because MarkRange is actually a half-opened interval
/// and during the query execution we receive some kind of request from every replica
/// to read some ranges from a specific part.
/// We have to avoid the situation, where some range is read twice.
/// This struct helps us to do it using only two operations (intersection and inversion)
/// over a set of half opened intervals.
struct HalfIntervals
{
    static HalfIntervals initializeWithEntireSpace()
    {
        auto left_inf = std::numeric_limits<decltype(MarkRange::begin)>::min();
        auto right_inf = std::numeric_limits<decltype(MarkRange::end)>::max();
        return HalfIntervals{{{left_inf, right_inf}}};
    }

    static HalfIntervals initializeFromMarkRanges(MarkRanges ranges)
    {
        OrderedRanges new_intervals;
        for (const auto & range : ranges)
            new_intervals.insert(range);

        return HalfIntervals{std::move(new_intervals)};
    }

    MarkRanges convertToMarkRangesFinal()
    {
        MarkRanges result;
        std::move(intervals.begin(), intervals.end(), std::back_inserter(result));
        return result;
    }

    HalfIntervals & intersect(const HalfIntervals & rhs)
    {
        /**
         * first   [   ) [   ) [   ) [  ) [  )
         * second    [       ) [ ) [   )  [    )
         */
        OrderedRanges intersected;

        const auto & first_intervals = intervals;
        auto first = first_intervals.begin();
        const auto & second_intervals = rhs.intervals;
        auto second = second_intervals.begin();

        while (first != first_intervals.end() && second != second_intervals.end())
        {
            auto curr_intersection = MarkRange{
                std::max(second->begin, first->begin),
                std::min(second->end, first->end)
            };

            /// Insert only if segments are intersect
            if (curr_intersection.begin < curr_intersection.end)
                intersected.insert(std::move(curr_intersection));

            if (first->end <= second->end)
                ++first;
            else
                ++second;
        }

        std::swap(intersected, intervals);

        return *this;
    }

    HalfIntervals & negate()
    {
        auto left_inf = std::numeric_limits<decltype(MarkRange::begin)>::min();
        auto right_inf = std::numeric_limits<decltype(MarkRange::end)>::max();

        if (intervals.empty())
        {
            intervals.insert(MarkRange{left_inf, right_inf});
            return *this;
        }

        OrderedRanges new_ranges;

        /// Possibly add (-inf; begin)
        if (auto begin = intervals.begin()->begin; begin != left_inf)
            new_ranges.insert(MarkRange{left_inf, begin});

        auto prev = intervals.begin();
        for (auto it = std::next(intervals.begin()); it != intervals.end(); ++it)
        {
            if (prev->end != it->begin)
                new_ranges.insert(MarkRange{prev->end, it->begin});
            prev = it;
        }

        /// Try to add (end; +inf)
        if (auto end = intervals.rbegin()->end; end != right_inf)
            new_ranges.insert(MarkRange{end, right_inf});

        std::swap(new_ranges, intervals);

        return *this;
    }

    bool operator==(const HalfIntervals & rhs) const
    {
        return intervals == rhs.intervals;
    }

    using OrderedRanges = std::set<MarkRange>;
    OrderedRanges intervals;
};


[[ maybe_unused ]] static std::ostream & operator<< (std::ostream & out, const HalfIntervals & ranges)
{
    for (const auto & range: ranges.intervals)
        out << fmt::format("({}, {}) ", range.begin, range.end);
    return out;
}

/// This is needed for tests where we don't need to modify objects
[[ maybe_unused ]] static HalfIntervals getIntersection(const HalfIntervals & first, const HalfIntervals & second)
{
    auto result = first;
    result.intersect(second);
    return result;
}

}
