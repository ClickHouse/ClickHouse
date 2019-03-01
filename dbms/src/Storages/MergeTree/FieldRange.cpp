#include <Storages/MergeTree/FieldRange.h>
#include <sstream>
#include <iostream>


namespace DB
{

    class IFunction;
    using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

    void applyFunction(
            const FunctionBasePtr & func,
            const DataTypePtr & arg_type, const Field & arg_value,
            DataTypePtr & res_type, Field & res_value)
    {
        res_type = func->getReturnType();

        Block block
                {
                        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
                        { nullptr, res_type, "y" }
                };

        func->execute(block, {0}, 1, 1);

        block.safeGetByPosition(1).column->get(0, res_value);
    }

    String Range::toString() const
    {
        std::stringstream str;

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

    RangeSet::RangeSet(const std::vector<DB::Range> &data): data(data)
    {
        normalize();
    }
    RangeSet::RangeSet(const DB::Range &range)
    {
        data = {range};
    }
    RangeSet::RangeSet() {}

    void RangeSet::normalize()
    {
        if (data.size() <= 1)
        {
            return;
        }
        std::sort(data.begin(), data.end(), [](const Range& lhs, const Range& rhs) {
            return !lhs.left_bounded ||
            (rhs.left_bounded && (Range::less(lhs.left, rhs.left)
            || (lhs.left_included && !rhs.left_included && Range::equals(lhs.left, rhs.right))
            ));
        });
        std::vector<Range> normalized;
        Field right_border;
        bool right_bounded = false;
        bool right_included = false;
        for (const auto& range : data)
        {
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
            }
            else
            {
                normalized.back().right = range.right;
                if (!range.right_bounded)
                {
                    normalized.back().right = Field();
                    normalized.back().right_bounded = false;
                    normalized.back().right_included = false;
                    break;
                }
            }
            right_border = range.right;
            right_included = range.right_included;
        }
        data = std::move(normalized);
    }

    RangeSet & RangeSet::operator |= (const RangeSet & rhs)
    {
        if (this != &rhs)
        {
            for (const auto &range : rhs.data)
            {
                data.push_back(range);
            }
            normalize();
        }
        return *this;
    }

    RangeSet RangeSet::operator | (const RangeSet & rhs) const
    {
        RangeSet tmp = *this;
        tmp |= rhs;
        return tmp;
    }

    bool RangeSet::intersectsRange(const Range &rhs) const
    {
        auto left_it = std::lower_bound(data.begin(), data.end(), rhs, [](const Range& element, const Range& value) {
            if (!value.left_bounded)
            {
                return false;
            }
            else if (element.leftThan(value.left) || (!value.left_included && (element.right_bounded && Range::equals(value.left, element.right))))
            {
                return true;
            }
            return false;
        });
        auto right_it = std::lower_bound(data.begin(), data.end(), rhs, [](const Range& element, const Range& value) {
            if (!value.right_bounded)
            {
                return true;
            }
            else if (element.rightThan(value.right) || (!value.right_included && (element.left_bounded && Range::equals(value.right, element.left))))
            {
                return false;
            }
            return true;
        });
        return left_it < right_it;
    }

    bool RangeSet::isContainedBy(const Range & rhs) const
    {
        if (data.empty())
        {
            return true;
        }
        return rhs.containsRange(*data.begin()) && rhs.containsRange(*data.rbegin());
    }

    std::optional<RangeSet> RangeSet::applyMonotonicFunction(
            const FunctionBasePtr & func,
            DataTypePtr & arg_type,
            DataTypePtr & res_type)
    {
        DataTypePtr new_type;
        std::vector<Range> result;
        for (auto range : data) {
            IFunction::Monotonicity monotonicity = func->getMonotonicityForRange(
                    *arg_type.get(), range.left, range.right);
            std::cerr << arg_type->getName() << "\n";

            if (!monotonicity.is_monotonic)
            {
                return {};
            }
            if (!range.left.isNull()) {
                applyFunction(func, arg_type, range.left, new_type, range.left);
            }
            if (!new_type)
            {
                return {};
            }
            if (!range.right.isNull())
            {
                applyFunction(func, arg_type, range.right, new_type, range.right);
            }
            if (!new_type)
            {
                return {};
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

    std::optional<RangeSet> RangeSet::applyInvertibleFunction(
            const FunctionBasePtr &func,
            size_t arg_index)
    {
        RangeSet result;
        for (const auto& range : data)
        {
            RangeSet tmp;
            bool inverted = func->invertRange(range, arg_index, func->getArgumentTypes(), tmp);
            if (inverted)
            {
                result |= tmp;
            }
            else
            {
                return {};
            }
        }
        result.normalize();
        return result;
    }
}
