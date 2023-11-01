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
        if (left.getType() == Field::Types::UInt64 && left.get<UInt64>() != std::numeric_limits<UInt64>::max())
        {
            ++left.get<UInt64 &>();
            left_included = true;
        }
        if (left.getType() == Field::Types::Int64 && left.get<Int64>() != std::numeric_limits<Int64>::max())
        {
            ++left.get<Int64 &>();
            left_included = true;
        }
    }
    if (right.isExplicit() && !right_included)
    {
        if (right.getType() == Field::Types::UInt64 && right.get<UInt64>() != std::numeric_limits<UInt64>::min())
        {
            --right.get<UInt64 &>();
            right_included = true;
        }
        if (right.getType() == Field::Types::Int64 && right.get<Int64>() != std::numeric_limits<Int64>::min())
        {
            --right.get<Int64 &>();
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

String Range::toString() const
{
    WriteBufferFromOwnString str;

    str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";
    str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
}

}
