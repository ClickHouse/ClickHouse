#include <Core/PlainRanges.h>

namespace DB
{

PlainRanges::PlainRanges(const Range & range)
{
    ranges.push_back(range);
}


PlainRanges::PlainRanges(const Ranges & ranges_, bool may_have_intersection, bool ordered)
{
    if (may_have_intersection)
        ranges = ordered ? makePlainFromOrdered(ranges_) : makePlainFromUnordered(ranges_);
    else
        ranges = ranges_;
}

Ranges PlainRanges::makePlainFromOrdered(const Ranges & ranges_)
{
    if (ranges_.size() <= 1)
        return ranges_;

    Ranges ret{ranges_.front()};

    for (size_t i = 1; i < ranges_.size(); ++i)
    {
        const auto & cur = ranges_[i];
        if (ret.back().intersectsRange(cur))
            ret.back() = *ret.back().unionWith(cur);
        else
            ret.push_back(cur);
    }

    return ret;
}

Ranges PlainRanges::makePlainFromUnordered(Ranges ranges_)
{
    if (ranges_.size() <= 1)
        return ranges_;

    std::sort(ranges_.begin(), ranges_.end(), compareByLeftBound);
    return makePlainFromOrdered(ranges_);
}

PlainRanges PlainRanges::unionWith(const PlainRanges & other)
{
    auto left_itr = ranges.begin();
    auto right_itr = other.ranges.begin();

    Ranges new_range;
    for (; left_itr != ranges.end() && right_itr != other.ranges.end();)
    {
        if (left_itr->leftThan(*right_itr))
        {
            new_range.push_back(*left_itr);
            left_itr++;
        }
        else if (left_itr->rightThan(*right_itr))
        {
            new_range.push_back(*right_itr);
            right_itr++;
        }
        else /// union
        {
            new_range.emplace_back(*(left_itr->unionWith(*right_itr)));
            if (compareByRightBound(*left_itr, *right_itr))
                left_itr++;
            else
                right_itr++;
        }
    }

    while (left_itr != ranges.end())
    {
        new_range.push_back(*left_itr);
        left_itr++;
    }

    while (right_itr != other.ranges.end())
    {
        new_range.push_back(*right_itr);
        right_itr++;
    }

    /// After union two PlainRanges, new ranges may like: [1, 4], [2, 5]
    /// We must make them plain.

    return PlainRanges(makePlainFromOrdered(new_range));
}

PlainRanges PlainRanges::intersectWith(const PlainRanges & other)
{
    auto left_itr = ranges.begin();
    auto right_itr = other.ranges.begin();

    Ranges new_ranges;
    for (; left_itr != ranges.end() && right_itr != other.ranges.end();)
    {
        if (left_itr->leftThan(*right_itr))
        {
            left_itr++;
        }
        else if (left_itr->rightThan(*right_itr))
        {
            right_itr++;
        }
        else /// intersection
        {
            auto intersected = left_itr->intersectWith(*right_itr);

            if (intersected) /// skip blank range
                new_ranges.emplace_back(*intersected);

            if (compareByRightBound(*left_itr, *right_itr))
                left_itr++;
            else
                right_itr++;
        }
    }
    return PlainRanges(new_ranges);
}

bool PlainRanges::compareByLeftBound(const Range & lhs, const Range & rhs)
{
    if (lhs.left == NEGATIVE_INFINITY && rhs.left == NEGATIVE_INFINITY)
        return false;
    return Range::less(lhs.left, rhs.left) || ((!lhs.left_included && rhs.left_included) && Range::equals(lhs.left, rhs.left));
};

bool PlainRanges::compareByRightBound(const Range & lhs, const Range & rhs)
{
    if (lhs.right == POSITIVE_INFINITY && rhs.right == POSITIVE_INFINITY)
        return false;
    return Range::less(lhs.right, rhs.right) || ((!lhs.right_included && rhs.right_included) && Range::equals(lhs.right, rhs.right));
};


std::vector<Ranges> PlainRanges::invert(const Ranges & to_invert_ranges)
{
    /// invert a blank ranges
    if (to_invert_ranges.empty())
        return {makeUniverse().ranges};

    std::vector<Ranges> reverted_ranges;
    for (const auto & range : to_invert_ranges)
    {
        if (range.isInfinite())
            /// return a blank ranges
            return {{}};
        reverted_ranges.push_back(range.invertRange());
    }
    return reverted_ranges;
};
}
