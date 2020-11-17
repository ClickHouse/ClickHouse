#pragma once

#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>

#include <Functions/IFunction.h>

namespace DB
{
/** A field, that can be stored in two representations:
 * - A standalone field.
 * - A field with reference to its position in a block.
 *   It's needed for execution of functions on ranges during
 *   index analysis. If function was executed once for field,
 *   its result would be cached for whole block for which field's reference points to.
 */
struct FieldRef : public Field
{
    FieldRef() = default;

    /// Create as explicit field without block.
    template <typename T>
    FieldRef(T && value) : Field(std::forward<T>(value)) {}

    /// Create as reference to field in block.
    FieldRef(Block * block_, size_t row_idx_, size_t column_idx_)
            : Field((*block_->getByPosition(column_idx_).column)[row_idx_]),
              block(block_), row_idx(row_idx_), column_idx(column_idx_) {}

    bool isExplicit() const { return block == nullptr; }

    Block * block = nullptr;
    size_t row_idx = 0;
    size_t column_idx = 0;
};

/** Range with open or closed ends; possibly unbounded.
*/
class Range
{
public:
    static bool equals(const Field & lhs, const Field & rhs);
    static bool less(const Field & lhs, const Field & rhs);

    FieldRef left;                       /// the left border, if any
    FieldRef right;                      /// the right border, if any
    bool left_bounded = false;        /// bounded at the left
    bool right_bounded = false;       /// bounded at the right
    bool left_included = false;       /// includes the left border, if any
    bool right_included = false;      /// includes the right border, if any

    /// The whole unversum.
    Range() {}

    /// One point.
    Range(const FieldRef & point)
            : left(point), right(point), left_bounded(true), right_bounded(true), left_included(true), right_included(true) {}

    /// A bounded two-sided range.
    Range(const FieldRef & left_, bool left_included_, const FieldRef & right_, bool right_included_)
            : left(left_), right(right_),
              left_bounded(true), right_bounded(true),
              left_included(left_included_), right_included(right_included_)
    {
        shrinkToIncludedIfPossible();
    }

    static Range createRightBounded(const FieldRef & right_point, bool right_included)
    {
        Range r;
        r.right = right_point;
        r.right_bounded = true;
        r.right_included = right_included;
        r.shrinkToIncludedIfPossible();
        return r;
    }

    static Range createLeftBounded(const FieldRef & left_point, bool left_included)
    {
        Range r;
        r.left = left_point;
        r.left_bounded = true;
        r.left_included = left_included;
        r.shrinkToIncludedIfPossible();
        return r;
    }

    /** Optimize the range. If it has an open boundary and the Field type is "loose"
      * - then convert it to closed, narrowing by one.
      * That is, for example, turn (0,2) into [1].
      */
    void shrinkToIncludedIfPossible()
    {
        if (left.isExplicit() && left_bounded && !left_included)
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
        if (right.isExplicit() && right_bounded && !right_included)
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

    bool empty() const
    {
        return left_bounded && right_bounded
               && (less(right, left)
                   || ((!left_included || !right_included) && !less(left, right)));
    }

    /// x contained in the range
    bool contains(const FieldRef & x) const
    {
        return !leftThan(x) && !rightThan(x);
    }

    /// x is to the left
    bool rightThan(const FieldRef & x) const
    {
        return (left_bounded
                ? !(less(left, x) || (left_included && equals(x, left)))
                : false);
    }

    /// x is to the right
    bool leftThan(const FieldRef & x) const
    {
        return (right_bounded
                ? !(less(x, right) || (right_included && equals(x, right)))
                : false);
    }

    bool intersectsRange(const Range & r) const
    {
        /// r to the left of me.
        if (r.right_bounded
            && left_bounded
            && (less(r.right, left)
                || ((!left_included || !r.right_included)
                    && equals(r.right, left))))
            return false;

        /// r to the right of me.
        if (r.left_bounded
            && right_bounded
            && (less(right, r.left)                          /// ...} {...
                || ((!right_included || !r.left_included)    /// ...) [... or ...] (...
                    && equals(r.left, right))))
            return false;

        return true;
    }

    bool containsRange(const Range & r) const
    {
        /// r starts to the left of me.
        if (left_bounded
            && (!r.left_bounded
                || less(r.left, left)
                || (r.left_included
                    && !left_included
                    && equals(r.left, left))))
            return false;

        /// r ends right of me.
        if (right_bounded
            && (!r.right_bounded
                || less(right, r.right)
                || (r.right_included
                    && !right_included
                    && equals(r.right, right))))
            return false;

        return true;
    }

    void swapLeftAndRight()
    {
        std::swap(left, right);
        std::swap(left_bounded, right_bounded);
        std::swap(left_included, right_included);
    }

    String toString() const;
};

class RangeSet
{
public:
    std::vector<Range> data;
    void normalize();
    RangeSet() = default;
    RangeSet(const Range & range);
    explicit RangeSet(const std::vector<Range> & data);

    RangeSet & operator |= (const RangeSet & rhs);
    RangeSet operator | (const RangeSet & rhs) const;
    RangeSet & operator |= (const Range &rhs);
    bool intersectsRange(const Range & rhs) const;
    bool isContainedBy(const Range & rhs) const;

    std::optional<RangeSet> applyMonotonicFunction(const FunctionBasePtr & func, DataTypePtr & arg_type, DataTypePtr & res_type);
    std::optional<RangeSet> applyInvertibleFunction(const FunctionBasePtr & func, size_t arg_index);

    String toString() const;
};


Field applyFunctionForField(const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value);
FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field);

}


