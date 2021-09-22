#pragma once

#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{


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

private:
    std::multiset<PartToRead, PartToRead::CompareByBegin> by_begin;
    std::multiset<PartToRead, PartToRead::CompareByEnd> by_end;
};



struct MarkRangesIntersectionsIndex
{
    void addRange(MarkRange range)
    {
        by_begin.insert(range);
        by_end.insert(range);
    }

    void addRanges(MarkRanges ranges)
    {
        for (auto & range : ranges)
            addRange(std::move(range));
    }

    size_t numberOfIntersectionsWith(MarkRange range)
    {
        /// Find the first one that starts with a larger coordinate
        auto right_iter = std::lower_bound(by_begin.begin(), by_begin.end(), range.end, MarkRangesIntersectionsIndex::FindByBeginGreater());

        /// Find the last that ends with a lower coordinate
        auto left_iter = std::lower_bound(by_end.rbegin(), by_end.rend(), range.begin, MarkRangesIntersectionsIndex::FindByEndLess());

        size_t result = by_begin.size();
        result -= std::distance(left_iter, by_end.rend());
        result -= std::distance(right_iter, by_begin.end());

        return result;
    }

    // MarkRanges findNonIntersectingMarkRanges(MarkRanges ranges)
    // {
    //     /// Find the first one that starts with a larger coordinate
    //     auto right_iter = std::lower_bound(by_begin.begin(), by_begin.end(), range.end, MarkRangesIntersectionsIndex::FindByBeginGreater());

    //     /// Find the last that ends with a lower coordinate
    //     auto left_iter = std::lower_bound(by_end.rbegin(), by_end.rend(), range.begin, MarkRangesIntersectionsIndex::FindByEndLess());

    //     MarkRanges result;
    //     for (auto it = left_iter; it != by_end.rend(); ++it)
    //         result.push_back(*it);
    //     for (auto it = right_iter; it != by_begin.end(); ++it)
    //         result.push_back(*it);
    //     return result;
    // }


    struct FindByBeginGreater : std::binary_function<bool, MarkRange, size_t>
    {
        bool operator()(const MarkRange & rhs, size_t value) const
        {
            return value > rhs.begin;
        }
    };

    struct FindByEndLess : std::binary_function<bool, MarkRange, size_t>
    {
        bool operator()(const MarkRange & lhs, size_t value) const
        {
            return value < lhs.end;
        }
    };

    struct CompareByBegin : std::binary_function<bool, MarkRange, MarkRange>
    {
        bool operator()(const MarkRange & lhs, const MarkRange & rhs) const
        {
            return lhs.begin < rhs.begin;
        }
    };


    struct CompareByEnd : std::binary_function<bool, MarkRange, MarkRange>
    {
        bool operator()(const MarkRange & lhs, const MarkRange & rhs) const
        {
            return lhs.end < rhs.end;
        }
    };

private:
    std::multiset<MarkRange, CompareByBegin> by_begin;
    std::multiset<MarkRange, CompareByEnd> by_end;

};


}
