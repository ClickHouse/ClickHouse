#include "ColumnFilter.h"
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
}

void Int64RangeFilter::testInt64Values(DB::RowSet & row_set, size_t offset, size_t len, const Int64 * data) const
{
    for (size_t t = 0; t < len; ++t)
    {
        row_set.set(offset + t, data[t] >= min && data[t] <= max);
    }
}

bool isFunctionNode(const ActionsDAG::Node & node)
{
    return node.function_base != nullptr;
}

bool isInputNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::INPUT;
}

bool isConstantNode(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::COLUMN;
}

bool isCompareColumnWithConst(const ActionsDAG::Node & node)
{
    if (!isFunctionNode(node))
        return false;
    // TODO: support or function
    if (node.function_base->getName() == "or")
        return false;
    size_t input_count = 0;
    size_t constant_count = 0;
    for (const auto & child : node.children)
    {
        if (isInputNode(*child))
            ++input_count;
        if (isConstantNode(*child))
            ++constant_count;
    }
    return input_count == 1 && constant_count >= 1;
}

const ActionsDAG::Node * getInputNode(const ActionsDAG::Node & node)
{
    for (const auto & child : node.children)
    {
        if (isInputNode(*child))
            return child;
    }
    throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "No input node found");
}

ActionsDAG::NodeRawConstPtrs getConstantNode(const ActionsDAG::Node & node)
{
    ActionsDAG::NodeRawConstPtrs result;
    for (const auto & child : node.children)
    {
        if (isConstantNode(*child))
            result.push_back(child);
    }
    return result;
}

ColumnFilterPtr Int64RangeFilter::create(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return nullptr;
    const auto * input_node = getInputNode(node);
    if (!isInt64(input_node->result_type))
        return nullptr;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    if (func_name == "equals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        return std::make_shared<Int64RangeFilter>(value, value, false);
    }
    else if (func_name == "less")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        return std::make_shared<Int64RangeFilter>(std::numeric_limits<Int64>::min(), value - 1, false);
    }
    else if (func_name == "greater")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        return std::make_shared<Int64RangeFilter>(value + 1, std::numeric_limits<Int64>::max(), false);
    }
    else if (func_name == "lessOrEquals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        return std::make_shared<Int64RangeFilter>(std::numeric_limits<Int64>::min(), value, true);
    }
    else if (func_name == "greaterOrEquals")
    {
        Int64 value = constant_nodes.front()->column->getInt(0);
        return std::make_shared<Int64RangeFilter>(value, std::numeric_limits<Int64>::max(), true);
    }
    return nullptr;
}


ColumnFilterPtr nullOrFalse(bool null_allowed) {
    if (null_allowed) {
        return std::make_shared<IsNullFilter>();
    }
    return std::make_unique<AlwaysFalseFilter>();
}

ColumnFilterPtr Int64RangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<Int64RangeFilter>(min, max, false);
        case Int64Range: {
            bool both_null_allowed = null_allowed && other->testNull();
            const auto * other_range = dynamic_cast<const Int64RangeFilter *>(other);
            if (other_range->min > max || other_range->max < min)
            {
                return nullOrFalse(both_null_allowed);
            }
            return std::make_shared<Int64RangeFilter>(
                std::min(min, other_range->min), std::max(max, other_range->max), both_null_allowed);
        }
        default:
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Can't merge filter of kind {}", magic_enum::enum_name(other->kind()));
    }
}
ColumnFilterPtr ByteValuesFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<ByteValuesFilter>(values, false);
        case ByteValues: {
            bool both_null_allowed = null_allowed && other->testNull();
            const auto & other_values = dynamic_cast<const ByteValuesFilter *>(other);
            // TODO: add string lower bound and upper bound to test always false fastly
            const ByteValuesFilter * small_filter = this;
            const ByteValuesFilter * large_filter = other_values;
            if (small_filter->values.size() > large_filter->values.size())
            {
                std::swap(small_filter, large_filter);
            }
            std::vector<String> new_values;
            new_values.reserve(small_filter->values.size());
            for (const auto & value : large_filter->values)
            {
                if (small_filter->values.contains(value))
                    new_values.push_back(value);
            }
            if (new_values.empty())
            {
                return nullOrFalse(both_null_allowed);
            }
            return std::make_shared<ByteValuesFilter>(new_values, both_null_allowed);
        }
        default:
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Can't merge filter of kind {}", magic_enum::enum_name(other->kind()));
    }
}

template <is_floating_point T>
ColumnFilterPtr FloatRangeFilter<T>::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<FloatRangeFilter<T>>(
                min, lower_unbounded, lower_exclusive, max, upper_unbounded, upper_exclusive, false);
        case FloatRange:
        case DoubleRange:
        {
            bool both_null_allowed = null_allowed && other->testNull();

            auto otherRange = static_cast<const FloatRangeFilter<T>*>(other);

            auto lower = std::max(min, otherRange->min);
            auto upper = std::min(max, otherRange->max);

            auto both_lower_unbounded =
                lower_unbounded && otherRange->lowerUnbounded_;
            auto both_upper_unbounded =
                upper_unbounded && otherRange->upperUnbounded_;

            auto lower_exclusive_ = !both_lower_unbounded &&
                (!testDouble(lower) || !other->testFloat64(lower));
            auto upper_exclusive_ = !both_upper_unbounded &&
                (!testDouble(upper) || !other->testFloat64(upper));

            if (lower > upper || (lower == upper && lower_exclusive)) {
                nullOrFalse(both_null_allowed);
            }
            return std::make_unique<FloatRangeFilter<T>>(
                lower,
                both_lower_unbounded,
                lower_exclusive_,
                upper,
                both_upper_unbounded,
                upper_exclusive_,
                both_null_allowed);
        }
        default:
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Can't merge filter of kind {}", magic_enum::enum_name(other->kind()));
    }
}

template <>
class FloatRangeFilter<Float32>;
template <>
class FloatRangeFilter<Float64>;
}
