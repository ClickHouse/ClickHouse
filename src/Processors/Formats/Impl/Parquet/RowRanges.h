#pragma once
#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
namespace DB
{

class RowRanges;

struct RowRange
{
    friend class RowRanges;

public:
    static std::optional<RowRange> unionRange(const RowRange & left, const RowRange & right)
    {
        if (left.from <= right.from)
        {
            if (left.to + 1 >= right.from)
                return RowRange{left.from, std::max(left.to, right.to)};
        }
        else if (right.to + 1 >= left.from)
        {
            return RowRange{right.from, std::max(left.to, right.to)};
        }
        return std::nullopt;
    }

    static std::optional<RowRange> intersection(const RowRange & left, const RowRange & right)
    {
        if (left.from <= right.from)
        {
            if (left.to >= right.from)
                return RowRange{right.from, std::min(left.to, right.to)};
        }
        else if (right.to >= left.from)
        {
            return RowRange{left.from, std::min(left.to, right.to)};
        }
        return std::nullopt; // Return a default Range object if no intersection range found
    }

    RowRange(const size_t from_, const size_t to_) : from(from_), to(to_) { assert(from <= to); }

    size_t count() const { return to - from + 1; }

    bool isBefore(const RowRange & other) const { return to < other.from; }

    bool isAfter(const RowRange & other) const { return from > other.to; }

    bool isIn(size_t point) const { return from <= point && point <= to; }

    size_t from;
    size_t to;
};

class RowRanges
{
    std::vector<RowRange> ranges;

public:
    RowRanges() = default;

    explicit RowRanges(const RowRange & range) { ranges.push_back(range); }
    explicit RowRanges(const std::vector<RowRange> & ranges_) : ranges(ranges_) { }

    static RowRanges createSingle(const size_t rowCount) { return RowRanges({RowRange(0L, rowCount - 1L)}); }

    static RowRanges unionRanges(const RowRanges & left, const RowRanges & right)
    {
        RowRanges result;
        auto it1_pair = std::make_pair(left.ranges.begin(), left.ranges.end());
        auto it2_pair = std::make_pair(right.ranges.begin(), right.ranges.end());
        if (it2_pair.first != it2_pair.second)
        {
            RowRange range2 = *it2_pair.first;
            while (it1_pair.first != it1_pair.second)
            {
                RowRange range1 = *it1_pair.first;
                if (range1.isAfter(range2))
                {
                    result.add(range2);
                    range2 = range1;
                    std::swap(it1_pair, it2_pair);
                }
                else
                {
                    result.add(range1);
                }
                ++it1_pair.first;
            }
            result.add(range2);
        }
        else
        {
            it2_pair = it1_pair;
        }
        while (it2_pair.first != it2_pair.second)
        {
            result.add(*it2_pair.first);
            ++it2_pair.first;
        }

        return result;
    }

    static RowRanges intersection(const RowRanges & left, const RowRanges & right)
    {
        RowRanges result;

        size_t rightIndex = 0;
        for (const RowRange & l : left.ranges)
        {
            for (size_t i = rightIndex, n = right.ranges.size(); i < n; ++i)
            {
                const RowRange & r = right.ranges[i];
                if (l.isBefore(r))
                {
                    break;
                }
                else if (l.isAfter(r))
                {
                    rightIndex = i + 1;
                    continue;
                }
                auto tmp = RowRange::intersection(l, r);
                result.add(tmp.value());
            }
        }
        return result;
    }

    RowRanges slice(const size_t from, const size_t to) const
    {
        RowRanges result;
        for (const RowRange & range : ranges)
            if (range.from >= from && range.to <= to)
                result.add(range);
        return result;
    }

    void add(const RowRange & range)
    {
        RowRange rangeToAdd = range;
        for (int i = static_cast<int>(ranges.size()) - 1; i >= 0; --i)
        {
            RowRange last = ranges[i];
            assert(!last.isAfter(range));
            const auto u = RowRange::unionRange(last, rangeToAdd);
            if (!u.has_value())
                break;
            rangeToAdd = u.value();
            ranges.erase(ranges.begin() + i);
        }
        ranges.push_back(rangeToAdd);
    }

    size_t rowCount() const
    {
        size_t cnt = 0;
        for (const RowRange & range : ranges)
            cnt += range.count();
        return cnt;
    }

    bool isOverlapping(const size_t from, const size_t to) const
    {
        const RowRange searchRange(from, to);
        const auto it = std::ranges::lower_bound(ranges, searchRange, [](const RowRange & r1, const RowRange & r2) { return r1.isBefore(r2); });
        return it != ranges.end() && !it->isAfter(searchRange);
    }

    std::string toString() const
    {
        std::string result = "[";
        for (const auto & range : ranges)
            result += "(" + std::to_string(range.from) + ", " + std::to_string(range.to) + "), ";
        if (!ranges.empty())
            result = result.substr(0, result.size() - 2);
        result += "]";
        return result;
    }

    const std::vector<RowRange> & getRanges() const { return ranges; }

    const RowRange & getRange(size_t index) const { return ranges[index]; }
};

using RowRangesPtr = std::shared_ptr<RowRanges>;
}
