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

    struct FindByBegin : std::binary_function<bool, Int64, PartToRead>
    {
        bool operator()(Int64 value, const PartToRead & rhs) const
        {
            return value < rhs.range.begin;
        }
    };

    struct FindByEndGreater : std::binary_function<bool, Int64, PartToRead>
    {
        bool operator()(Int64 value, const PartToRead & lhs) const
        {
            return value > lhs.range.end;
        }
    };

    struct CompareByBegin : std::binary_function<bool, PartToRead, PartToRead>
    {
        bool operator()(const PartToRead & lhs, const PartToRead & rhs) const
        {
            return lhs.range.begin < rhs.range.begin;
        }
    };


    struct CompareByEnd : std::binary_function<bool, PartToRead, PartToRead>
    {
        bool operator()(const PartToRead & lhs, const PartToRead & rhs) const
        {
            return lhs.range.end < rhs.range.end;
        }
    };
};


class PartRangesIntersectionsIndex
{
public:
    void addPart(PartToRead part)
    {
        by_begin.insert(part);
        by_end.insert(part);
    }

    size_t numberOfIntersectionsWith(PartBlockRange range)
    {
        /// Find the first one that starts with a larger coordinate
        auto right_iter = std::upper_bound(by_begin.begin(), by_begin.end(), range.end, PartToRead::FindByBegin());

        /// Find the last that ends with a lower coordinate
        auto left_iter = std::upper_bound(by_end.rbegin(), by_end.rend(), range.begin, PartToRead::FindByEndGreater());

        size_t result = by_begin.size();
        result -= std::distance(left_iter, by_end.rend());
        result -= std::distance(right_iter, by_begin.end());

        return result;
    }

    bool checkPartIsSuitable(PartToRead part)
    {
        auto number = numberOfIntersectionsWith(part.range);
        if (number == 0)
            return true;
        if (number == 1)
        {
            auto it = by_begin.find(part);
            if (it != by_begin.end() && it->name == part.name)
                return true;
        }

        return false;
    }

    String describe() const
    {
        String result = "[";
        for (const auto & it : by_begin)
            result += fmt::format("({} {} {}), ", it.name, it.range.begin, it.range.end);
        result += "]";
        return result;
    }


    void checkConsistencyOrThrow() const
    {
        Int64 prev_end = std::numeric_limits<Int64>::min();
        for (const auto & it : by_begin)
        {
            if (it.range.begin < prev_end)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersecting marks");
            prev_end = it.range.end;
        }
    }

private:
    std::multiset<PartToRead, PartToRead::CompareByBegin> by_begin;
    std::multiset<PartToRead, PartToRead::CompareByEnd> by_end;
};



struct MarkRangesIntersectionsIndex
{
    void addRange(MarkRange range)
    {
        by_begin.insert(range);
        // by_end.insert(range);
    }

    void addRanges(MarkRanges ranges)
    {
        for (auto & range : ranges)
            addRange(std::move(range));
    }

    size_t numberOfIntersectionsWith(MarkRange range)
    {
        /// Find the first one that starts with a larger coordinate
        /// In terms of std::upper_bound we will find an iterator since which the predicate is true
        auto right_iter = std::upper_bound(by_begin.begin(), by_begin.end(), range.end, MarkRangesIntersectionsIndex::StartsInCoordinateGreaterOrEqualThanValue());

        /// Find the last that ends with a lower coordinate
        auto left_iter = std::upper_bound(by_begin.rbegin(), by_begin.rend(), range.begin, MarkRangesIntersectionsIndex::EndsInCoordinateLessOrEqualThanValue());

        size_t result = by_begin.size();
        result -= std::distance(left_iter, by_begin.rend());
        result -= std::distance(right_iter, by_begin.end());

        return result;
    }

    std::vector<MarkRange> getIntersectingRanges(MarkRange range)
    {
        /// Find the first one that starts with a larger coordinate
        auto right_iter = std::upper_bound(by_begin.begin(), by_begin.end(), range.end,
            MarkRangesIntersectionsIndex::StartsInCoordinateGreaterOrEqualThanValue());

        /// Find the last that ends with a lower coordinate
        auto left_iter = std::upper_bound(by_begin.rbegin(), by_begin.rend(), range.begin,
            MarkRangesIntersectionsIndex::EndsInCoordinateLessOrEqualThanValue());

        std::vector<MarkRange> result;
        for (auto it = left_iter.base(); it != right_iter; ++it)
            result.push_back(*it);

        return result;
    }

    std::vector<MarkRange> getNewRanges(MarkRange range)
    {
        auto ranges = getIntersectingRanges(range);
        std::sort(ranges.begin(), ranges.end(), CompareByBegin());

        if (ranges.empty())
            throw std::runtime_error("Ranges empty!1");

        std::vector<MarkRange> gaps;

        if (range.begin < ranges.front().begin)
            gaps.push_back(MarkRange{range.begin, ranges.front().begin});

        auto prev_end = ranges.front().end;
        for (auto it = std::next(ranges.begin()); it != ranges.end(); ++it)
        {
            /// Do not add empty gaps
            if (prev_end != it->begin)
                gaps.push_back(MarkRange{prev_end, it->begin});
            prev_end = it->end;
        }

        if (range.end > ranges.back().end)
            gaps.push_back(MarkRange{ranges.back().end, range.end});

        return gaps;
    }

    /// Mark range is represented by a half-opened interval, the predicate checks the equility of boundaries
    struct StartsInCoordinateGreaterOrEqualThanValue
    {
        bool operator()(size_t value, const MarkRange & rhs) const
        {
            return rhs.begin >= value;
        }
    };

    struct EndsInCoordinateLessOrEqualThanValue
    {
        bool operator()(size_t value, const MarkRange & lhs) const
        {
            return lhs.end <= value;
        }
    };

    struct CompareByBegin
    {
        bool operator()(const MarkRange & lhs, const MarkRange & rhs) const
        {
            if (lhs.begin < rhs.begin)
                return true;
            return lhs.end < rhs.end;
        }
    };


    struct CompareByEnd
    {
        bool operator()(const MarkRange & lhs, const MarkRange & rhs) const
        {
            if (lhs.end < rhs.end)
                return true;
            return lhs.begin < rhs.begin;
        }
    };


    String describe() const
    {
        String result = "Mark ranges: ";
        for (const auto & it : by_begin)
            result += fmt::format("({}, {}) ", it.begin, it.end);
        result += "End! ";
        return result;
    }


    void checkConsistencyOrThrow() const
    {
        UInt64 prev_end = 0;
        for (const auto & it : by_begin)
        {
            if (it.begin < prev_end)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersecting marks");
            prev_end = it.end;
        }

        std::multiset<MarkRange, CompareByEnd> by_end;
        for (const auto & it : by_begin)
            by_end.insert(it);

        auto it = by_end.begin();
        for (const auto & begin_it: by_begin)
        {
            if (*it  != begin_it)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersecting marks");
            ++it;
        }
    }

private:
    std::multiset<MarkRange, CompareByBegin> by_begin;
};


}
