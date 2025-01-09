#include <Core/Range.h>
#include <Common/FieldVisitorToString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

Range::Range(const FieldRef & point) /// NOLINT
    : left(point), right(point), left_included(true), right_included(true) {}

/// A bounded two-sided range.
Range::Range(const FieldRef & left_, bool left_included_, const FieldRef & right_, bool right_included_)
    : left(left_)
    , right(right_)
    , left_included(left_included_)
    , right_included(right_included_)
{
    shrinkToIncludedIfPossible();
}

Range Range::createWholeUniverse()
{
    return Range(NEGATIVE_INFINITY, true, POSITIVE_INFINITY, true);
}

Range Range::createWholeUniverseWithoutNull()
{
    return Range(NEGATIVE_INFINITY, false, POSITIVE_INFINITY, false);
}

Range Range::createRightBounded(const FieldRef & right_point, bool right_included, bool with_null)
{
    Range r = with_null ? createWholeUniverse() : createWholeUniverseWithoutNull();
    r.right = right_point;
    r.right_included = right_included;
    r.shrinkToIncludedIfPossible();
    // Special case for [-Inf, -Inf]
    if (r.right.isNegativeInfinity() && right_included)
        r.left_included = true;
    return r;
}

Range Range::createLeftBounded(const FieldRef & left_point, bool left_included, bool with_null)
{
    Range r = with_null ? createWholeUniverse() : createWholeUniverseWithoutNull();
    r.left = left_point;
    r.left_included = left_included;
    r.shrinkToIncludedIfPossible();
    // Special case for [+Inf, +Inf]
    if (r.left.isPositiveInfinity() && left_included)
        r.right_included = true;
    return r;
}

/** Optimize the range. If it has an open boundary and the Field type is "loose"
  * - then convert it to closed, narrowing by one.
  * That is, for example, turn (0,2) into [1].
  */
void Range::shrinkToIncludedIfPossible()
{
    if (left.isExplicit() && !left_included)
    {
        if (left.getType() == Field::Types::UInt64 && left.safeGet<UInt64>() != std::numeric_limits<UInt64>::max())
        {
            ++left.safeGet<UInt64 &>();
            left_included = true;
        }
        if (left.getType() == Field::Types::Int64 && left.safeGet<Int64>() != std::numeric_limits<Int64>::max())
        {
            ++left.safeGet<Int64 &>();
            left_included = true;
        }
    }
    if (right.isExplicit() && !right_included)
    {
        if (right.getType() == Field::Types::UInt64 && right.safeGet<UInt64>() != std::numeric_limits<UInt64>::min())
        {
            --right.safeGet<UInt64 &>();
            right_included = true;
        }
        if (right.getType() == Field::Types::Int64 && right.safeGet<Int64>() != std::numeric_limits<Int64>::min())
        {
            --right.safeGet<Int64 &>();
            right_included = true;
        }
    }
}

bool Range::equals(const Field & lhs, const Field & rhs)
{
    return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs);
}

bool Range::less(const Field & lhs, const Field & rhs)
{
    return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs);
}

bool Range::empty() const
{
    return less(right, left)
        || ((!left_included || !right_included)
            && !less(left, right));
}

/// x contained in the range
bool Range::contains(const FieldRef & x) const
{
    return !leftThan(x) && !rightThan(x);
}

/// x is to the left
bool Range::rightThan(const FieldRef & x) const
{
    return less(left, x) || (left_included && equals(x, left));
}

/// x is to the right
bool Range::leftThan(const FieldRef & x) const
{
    return less(x, right) || (right_included && equals(x, right));
}

bool Range::rightThan(const Range & x) const
{
    return less(x.right, left) || (!(left_included && x.right_included) && equals(left, x.right));
}

bool Range::leftThan(const Range & x) const
{
    return less(right, x.left) || (!(x.left_included && right_included) && equals(right, x.left));
}

bool Range::fullBounded() const
{
    return left.getType() != Field::Types::Null && right.getType() != Field::Types::Null;
}

/// (-inf, +inf)
bool Range::isInfinite() const
{
    return left.isNegativeInfinity() && right.isPositiveInfinity();
}

bool Range::intersectsRange(const Range & r) const
{
    /// r to the left of me.
    if (less(r.right, left) || ((!left_included || !r.right_included) && equals(r.right, left)))
        return false;

    /// r to the right of me.
    if (less(right, r.left) || ((!right_included || !r.left_included) && equals(r.left, right)))
        return false;

    return true;
}

bool Range::containsRange(const Range & r) const
{
    /// r starts to the left of me.
    if (less(r.left, left) || (r.left_included && !left_included && equals(r.left, left)))
        return false;

    /// r ends right of me.
    if (less(right, r.right) || (r.right_included && !right_included && equals(r.right, right)))
        return false;

    return true;
}

void Range::invert()
{
    std::swap(left, right);
    if (left.isPositiveInfinity())
        left = NEGATIVE_INFINITY;
    if (right.isNegativeInfinity())
        right = POSITIVE_INFINITY;
    std::swap(left_included, right_included);
}

Ranges Range::invertRange() const
{
    Ranges ranges;
    /// For full bounded range will generate two ranges.
    if (fullBounded()) /// case: [1, 3] -> (-inf, 1), (3, +inf)
    {
        ranges.push_back({NEGATIVE_INFINITY, false, left, !left_included});
        ranges.push_back({right, !right_included, POSITIVE_INFINITY, false});
    }
    else if (isInfinite())
    {
        /// blank ranges
    }
    else /// case: (-inf, 1] or [1, +inf)
    {
        Range r = *this;
        std::swap(r.left, r.right);
        if (r.left.isPositiveInfinity()) /// [1, +inf)
        {
            r.left = NEGATIVE_INFINITY;
            r.right_included = !r.left_included;
            r.left_included = false;
        }
        else if (r.right.isNegativeInfinity()) /// (-inf, 1]
        {
            r.right = POSITIVE_INFINITY;
            r.left_included = !r.right_included;
            r.right_included = false;
        }
        ranges.push_back(r);
    }
    return ranges;
}

std::optional<Range> Range::intersectWith(const Range & r) const
{
    if (!intersectsRange(r))
        return {};

    bool left_bound_use_mine = true;
    bool right_bound_use_mine = true;

    if (less(left, r.left) || ((!left_included && r.left_included) && equals(left, r.left)))
        left_bound_use_mine = false;

    if (less(r.right, right) || ((!r.right_included && right_included) && equals(r.right, right)))
        right_bound_use_mine = false;

    return Range(
        left_bound_use_mine ? left : r.left,
        left_bound_use_mine ? left_included : r.left_included,
        right_bound_use_mine ? right : r.right,
        right_bound_use_mine ? right_included : r.right_included);
}

std::optional<Range> Range::unionWith(const Range & r) const
{
    if (!intersectsRange(r) && !nearByWith(r))
        return {};

    bool left_bound_use_mine = false;
    bool right_bound_use_mine = false;

    if (less(left, r.left) || ((!left_included && r.left_included) && equals(left, r.left)))
        left_bound_use_mine = true;

    if (less(r.right, right) || ((!r.right_included && right_included) && equals(r.right, right)))
        right_bound_use_mine = true;

    return Range(
        left_bound_use_mine ? left : r.left,
        left_bound_use_mine ? left_included : r.left_included,
        right_bound_use_mine ? right : r.right,
        right_bound_use_mine ? right_included : r.right_included);
}

bool Range::nearByWith(const Range & r) const
{
    /// me locates at left
    if (((right_included && !r.left_included) || (!right_included && r.left_included)) && equals(right, r.left))
        return true;

    /// r locate left
    if (((r.right_included && !left_included) || (r.right_included && !left_included)) && equals(r.right, left))
        return true;

    return false;
}

Range intersect(const Range & a, const Range & b)
{
    Range res = Range::createWholeUniverse();

    if (Range::less(a.left, b.left))
    {
        res.left = b.left;
        res.left_included = b.left_included;
    }
    else if (Range::equals(a.left, b.left))
    {
        res.left = a.left;
        res.left_included = a.left_included && b.left_included;
    }
    else
    {
        res.left = a.left;
        res.left_included = a.left_included;
    }

    if (Range::less(a.right, b.right))
    {
        res.right = a.right;
        res.right_included = a.right_included;
    }
    else if (Range::equals(a.right, b.right))
    {
        res.right = a.right;
        res.right_included = a.right_included && b.right_included;
    }
    else
    {
        res.right = b.right;
        res.right_included = b.right_included;
    }

    if (res.empty())
    {
        res.right = res.left;
        res.right_included = false;
        res.left_included = false;
    }

    return res;
}

String Range::toString() const
{
    WriteBufferFromOwnString str;

    str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";
    str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
}

Hyperrectangle intersect(const Hyperrectangle & a, const Hyperrectangle & b)
{
    size_t result_size = std::min(a.size(), b.size());

    Hyperrectangle res;
    res.reserve(result_size);

    for (size_t i = 0; i < result_size; ++i)
        res.push_back(intersect(a[i], b[i]));

    return res;
}

String toString(const Hyperrectangle & x)
{
    WriteBufferFromOwnString str;

    bool first = true;
    for (const auto & range : x)
    {
        if (!first)
            str << " × ";
        str << range.toString();
        first = false;
    }

    return str.str();
}

}
