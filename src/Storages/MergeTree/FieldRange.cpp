#include <Storages/MergeTree/FieldRange.h>

#include <sstream>
#include <iostream>
#include <Core/iostream_debug_helpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

class IFunction;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;


String Range::toString() const
{
    std::stringstream str;
    str.exceptions(std::ios::failbit);

    if (!left_bounded)
        str << "(-inf, ";
    else
        str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left)
            << ", ";

    if (!right_bounded)
        str << "+inf)";
    else
        str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');


    return str.str();
}

bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }

RangeSet::RangeSet(const std::vector<DB::Range> & data_): data(data_)
{
    normalize();
}
RangeSet::RangeSet(const DB::Range & range)
{
    data = {range};
}

/// Sort and merge intersecting ranges
void RangeSet::normalize()
{
    if (data.size() <= 1)
    {
        return;
    }
    std::sort(data.begin(), data.end(), [](const Range & lhs, const Range & rhs)
    {
        return !lhs.left_bounded ||
        (rhs.left_bounded && (Range::less(lhs.left, rhs.left)
        || (lhs.left_included && !rhs.left_included && Range::equals(lhs.left, rhs.left))
        ));
    });
    std::vector<Range> normalized;
    FieldRef right_border;
    bool right_bounded = false;
    bool right_included = false;
    for (const auto & range : data)
    {
        // Adding a new range, when it doesn't intersect the current rightmost range
        if (
                !right_bounded ||
                Range::less(right_border, range.left) ||
                ((!range.left_included || !right_included) && Range::equals(right_border, range.left)))
        {
            normalized.push_back(range);
            if (!range.right_bounded)
            {
                break;
            }
            right_bounded = true;
            right_border = range.right;
            right_included = range.right_included;
        }
        // Adding an extension of the rightmost range
        else
        {
            if (!range.right_bounded)
            {
                normalized.back().right = Field();
                normalized.back().right_bounded = false;
                normalized.back().right_included = false;
                break;
            }
            else if (
                    Range::less(right_border, range.right) ||
                    (!right_included && range.right_included &&
                    Range::equals(right_border, range.right)))
            {
                right_border = range.right;
                right_included = range.right_included;
                normalized.back().right = range.right;
            }
        }

    }
    data = std::move(normalized);
}

/// Intersect two sets of ranges
RangeSet & RangeSet::operator |= (const RangeSet & rhs)
{
    if (this != &rhs)
    {
        if (data.empty())
        {
            data = rhs.data;
        }
        else
        {
            for (const auto & range : rhs.data)
            {
                data.push_back(range);
            }
            normalize();
        }
    }
    return *this;
}

RangeSet RangeSet::operator | (const RangeSet & rhs) const
{
    RangeSet tmp = *this;
    tmp |= rhs;
    return tmp;
}

/// Intersect a set of ranges with a single range
RangeSet & RangeSet::operator |= (const Range & rhs)
{
    return ((*this) |= RangeSet(rhs));
}

/// Check whether a range intersects a set of ranges
bool RangeSet::intersectsRange(const Range & rhs) const
{
    auto cmp_left = [](const Range & element, const Range & value)
    {
        if (!value.left_bounded)
        {
            return false;
        }
        else if (element.leftThan(value.left) || (!value.left_included && (element.right_bounded && Range::equals(value.left, element.right))))
        {
            return true;
        }
        return false;
    };
    // auto left_it = std::lower_bound(data.begin(), data.end(), rhs, cmp_left);

    /* This is a temporary fix, due to a bug in some versions of libc++
     * see fix at: http://llvm.org/viewvc/llvm-project?view=revision&revision=345434*/

    int lp = -1, rp = static_cast<int>(data.size());
    while (rp - lp > 1)
    {
        int m = lp + (rp - lp) / 2;
        if (cmp_left(data[m], rhs))
        {
            lp = m;
        }
        else
        {
            rp = m;
        }
    }
    auto left_it = rp;
    auto cmp_right = [](const Range & element, const Range & value)
    {
        if (!value.right_bounded)
        {
            return true;
        }
        else if (element.rightThan(value.right) || (!value.right_included && (element.left_bounded && Range::equals(value.right, element.left))))
        {
            return false;
        }
        return true;
    };

    // auto right_it = std::lower_bound(data.begin(), data.end(), rhs, cmp_right);

    lp = -1;
    rp = static_cast<int>(data.size());
    while (rp - lp > 1)
    {
        int m = lp + (rp - lp) / 2;
        if (cmp_right(data[m], rhs))
        {
            lp = m;
        }
        else
        {
            rp = m;
        }
    }
    auto right_it = rp;
    return left_it < right_it;
}

/// Check whether range set is contained by a single range
bool RangeSet::isContainedBy(const Range & rhs) const
{
    if (data.empty())
    {
        return true;
    }

    return rhs.containsRange(*data.begin()) && rhs.containsRange(*data.rbegin());
}

/// Apply a monotonic function to a set of ranges
std::optional<RangeSet> RangeSet::applyMonotonicFunction(
        const FunctionBasePtr & func,
        DataTypePtr & arg_type,
        DataTypePtr & res_type)
{
    auto new_type = func->getReturnType();
    if (!new_type)
        return {};
    std::vector<Range> result;
    for (auto range : data)
    {
        auto monotonicity = func->getMonotonicityForRange(
                *arg_type.get(), range.left, range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }
        if (!range.left.isNull())
        {
            range.left = applyFunction(func, arg_type, range.left);
        }
        if (!range.right.isNull())
        {
            range.right = applyFunction(func, arg_type, range.right);
        }
        if (!monotonicity.is_positive)
        {
            range.swapLeftAndRight();
        }
        result.push_back(range);
    }
    res_type.swap(new_type);
    return RangeSet(result);
}

/// Apply an invertible function to a set of ranges
std::optional<RangeSet> RangeSet::applyInvertibleFunction(
        const FunctionBasePtr & func,
        size_t arg_index)
{
    std::cerr << "KEK: inverting\n";
    RangeSet result;
    for (const auto & range : data)
    {
        RangeSet tmp;
        bool inverted = func->invertRange(range, arg_index, func->getArgumentTypes(), tmp);
        std::cout << "KEK: " << inverted << "\n";
        std::cout << "KEK: " << tmp.toString() << "\n";
        if (inverted)
        {
            result |= tmp;
        }
        else
        {
            return {};
        }
    }
    return result;
}

String RangeSet::toString() const
{
    std::stringstream str;
    str << "{";
    for (size_t i = 0; i < data.size(); ++i)
    {
        if (i != 0)
            str << ", ";
        str << data[i].toString();
    }
    str << "}";
    return str.str();
}


Field applyFunctionForField(const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value)
{
    ColumnsWithTypeAndName columns
    {
        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
    };

    auto col = func->execute(columns, func->getResultType(), 1);
    return (*col)[0];
}


FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
    {
        if ((*columns)[i].name == result_name)
            result_idx = i;
    }

    ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
    if (result_idx == columns->size())
    {
        field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

}
