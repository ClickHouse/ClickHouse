#pragma once

#include <cstddef>
#include <utility>

#include <Columns/IColumn.h>
#include <base/defines.h>
#include <base/types.h>


namespace DB
{

/// A reference to a value inside a column or one of the special +/-Inf values.
/// Used to avoid Field during index analysis.
struct ColumnValueRef
{
    enum class Special : UInt8
    {
        Normal = 0,
        NegativeInfinity = 1,
        PositiveInfinity = 2,
    };

    const IColumn * column = nullptr;
    size_t row = 0;
    Special special = Special::Normal;

    static ColumnValueRef normal(const IColumn * column_, size_t row_) { return {column_, row_, Special::Normal}; }
    static ColumnValueRef negativeInfinity() { return {nullptr, 0, Special::NegativeInfinity}; }
    static ColumnValueRef positiveInfinity() { return {nullptr, 0, Special::PositiveInfinity}; }

    bool isNormal() const { return special == Special::Normal; }
    bool isNegativeInfinity() const { return special == Special::NegativeInfinity; }
    bool isPositiveInfinity() const { return special == Special::PositiveInfinity; }
    bool isInfinity() const { return isNegativeInfinity() || isPositiveInfinity(); }
    bool isNullAt() const { return isNormal() && column && column->isNullAt(row); }

    int compare(const ColumnValueRef & rhs, int nan_direction_hint = 1) const
    {
        if (isNegativeInfinity())
            return rhs.isNegativeInfinity() ? 0 : -1;
        if (isPositiveInfinity())
            return rhs.isPositiveInfinity() ? 0 : +1;
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return -1;

        chassert(column && rhs.column);
        return column->compareAt(row, rhs.row, *rhs.column, nan_direction_hint);
    }
};

/// Range with ends represented as ColumnValueRef
struct RangeRef
{
    ColumnValueRef left;
    ColumnValueRef right;
    bool left_included = false;
    bool right_included = false;

    RangeRef() = default;
    explicit RangeRef(const ColumnValueRef & point); /// NOLINT
    RangeRef(const ColumnValueRef & left_, bool left_included_, const ColumnValueRef & right_, bool right_included_);

    static RangeRef createWholeUniverse();
    static RangeRef createWholeUniverse(bool with_null);
    static RangeRef createWholeUniverseWithoutNull();
    static RangeRef createRightBounded(const ColumnValueRef & right_point, bool right_included, bool with_null = false);
    static RangeRef createLeftBounded(const ColumnValueRef & left_point, bool left_included, bool with_null = false);

    void invert();

    bool empty() const;
    bool isInfinite() const;
    bool intersectsRange(const RangeRef & r) const;
    bool containsRange(const RangeRef & r) const;
};

inline RangeRef::RangeRef(const ColumnValueRef & point) /// NOLINT
    : left(point)
    , right(point)
    , left_included(true)
    , right_included(true)
{
}

inline RangeRef::RangeRef(const ColumnValueRef & left_, bool left_included_, const ColumnValueRef & right_, bool right_included_)
    : left(left_)
    , right(right_)
    , left_included(left_included_)
    , right_included(right_included_)
{
}

inline RangeRef RangeRef::createWholeUniverse()
{
    return RangeRef(ColumnValueRef::negativeInfinity(), true, ColumnValueRef::positiveInfinity(), true);
}

inline RangeRef RangeRef::createWholeUniverse(bool with_null)
{
    return with_null ? createWholeUniverse() : createWholeUniverseWithoutNull();
}

inline RangeRef RangeRef::createWholeUniverseWithoutNull()
{
    return RangeRef(ColumnValueRef::negativeInfinity(), false, ColumnValueRef::positiveInfinity(), false);
}

inline RangeRef RangeRef::createRightBounded(const ColumnValueRef & right_point, bool right_included, bool with_null)
{
    RangeRef r = with_null ? createWholeUniverse() : createWholeUniverseWithoutNull();
    r.right = right_point;
    r.right_included = right_included;
    // Special case for [-Inf, -Inf]
    if (r.right.isNegativeInfinity() && right_included)
        r.left_included = true;
    return r;
}

inline RangeRef RangeRef::createLeftBounded(const ColumnValueRef & left_point, bool left_included, bool with_null)
{
    RangeRef r = with_null ? createWholeUniverse() : createWholeUniverseWithoutNull();
    r.left = left_point;
    r.left_included = left_included;
    // Special case for [+Inf, +Inf]
    if (r.left.isPositiveInfinity() && left_included)
        r.right_included = true;
    return r;
}

inline bool RangeRef::intersectsRange(const RangeRef & r) const
{
    /// r to the left of this.
    const int cmp1 = r.right.compare(left);
    if (cmp1 < 0 || (cmp1 == 0 && (!left_included || !r.right_included)))
        return false;

    /// r to the right of this.
    const int cmp2 = right.compare(r.left);
    if (cmp2 < 0 || (cmp2 == 0 && (!right_included || !r.left_included)))
        return false;

    return true;
}

inline bool RangeRef::containsRange(const RangeRef & r) const
{
    /// r starts to the left of this.
    const int cmp1 = r.left.compare(left);
    if (cmp1 < 0 || (cmp1 == 0 && r.left_included && !left_included))
        return false;

    /// r ends right of this.
    const int cmp2 = right.compare(r.right);
    if (cmp2 < 0 || (cmp2 == 0 && r.right_included && !right_included))
        return false;

    return true;
}

inline void RangeRef::invert()
{
    std::swap(left, right);
    if (left.isPositiveInfinity())
        left = ColumnValueRef::negativeInfinity();
    if (right.isNegativeInfinity())
        right = ColumnValueRef::positiveInfinity();
    std::swap(left_included, right_included);
}

inline bool RangeRef::empty() const
{
    const int cmp = right.compare(left);
    if (cmp < 0)
        return true;
    if (cmp > 0)
        return false;
    return !left_included || !right_included;
}

inline bool RangeRef::isInfinite() const
{
    return left.isNegativeInfinity() && right.isPositiveInfinity();
}

}
