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
    String name;

    bool operator==(const PartToRead & rhs) const
    {
        return range == rhs.range && name == rhs.name;
    }

    bool operator<(const PartToRead & rhs) const
    {
        /// We allow only consecutive non-intersecting ranges
        if (rhs.range.begin > range.begin && rhs.range.begin < range.end)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad ranges");
        return range.begin < rhs.range.begin && range.end <= rhs.range.begin;
    }
};


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
                /// It could happens if we have merged part on one replica
                /// but not on another.
                if (segment.name != part.name)
                    return IntersectionResult::REJECT;

                intersected_before = true;
            }
        }

        return intersected_before ? IntersectionResult::EXACTLY_ONE_INTERSECTION : IntersectionResult::NO_INTERSECTION;
    }

    using OrderedSegments = std::set<PartToRead>;
    OrderedSegments segments;
};


// class PartRangesIntersectionsIndex
// {
// public:
//     void addPart(PartToRead part)
//     {
//         by_begin.insert(part);
//         by_end.insert(part);
//     }

//     size_t numberOfIntersectionsWith(PartBlockRange range)
//     {
//         /// Find the first one that starts with a larger coordinate
//         auto right_iter = std::upper_bound(by_begin.begin(), by_begin.end(), range.end, PartToRead::FindByBegin());

//         /// Find the last that ends with a lower coordinate
//         auto left_iter = std::upper_bound(by_end.rbegin(), by_end.rend(), range.begin, PartToRead::FindByEndGreater());

//         size_t result = by_begin.size();
//         result -= std::distance(left_iter, by_end.rend());
//         result -= std::distance(right_iter, by_begin.end());

//         return result;
//     }

//     bool checkPartIsSuitable(PartToRead part)
//     {
//         auto number = numberOfIntersectionsWith(part.range);
//         if (number == 0)
//             return true;
//         if (number == 1)
//         {
//             auto it = by_begin.find(part);
//             if (it != by_begin.end() && it->name == part.name)
//                 return true;
//         }

//         return false;
//     }

//     String describe() const
//     {
//         String result = "[";
//         for (const auto & it : by_begin)
//             result += fmt::format("({} {} {}), ", it.name, it.range.begin, it.range.end);
//         result += "]";
//         return result;
//     }


//     void checkConsistencyOrThrow() const
//     {
//         Int64 prev_end = std::numeric_limits<Int64>::min();
//         for (const auto & it : by_begin)
//         {
//             if (it.range.begin < prev_end)
//                 throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersecting marks");
//             prev_end = it.range.end;
//         }
//     }

// private:
//     std::multiset<PartToRead, PartToRead::CompareByBegin> by_begin;
//     std::multiset<PartToRead, PartToRead::CompareByEnd> by_end;
// };



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
            auto are_intersect = [](auto & x, auto & y)
            {
                if ((x->begin <= y->begin) && (y->begin < x->end))
                    return true;
                if ((y->begin <= x->begin) && (x->begin < y->end))
                    return true;
                return false;
            };

            if (are_intersect(first, second))
            {
                intersected.insert(MarkRange{
                    std::max(second->begin, first->begin),
                    std::min(second->end, first->end)
                });
            }

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
        OrderedRanges new_ranges;

        /// Possibly add (-inf; begin)
        if (!intervals.empty())
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
        if (!intervals.empty())
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
