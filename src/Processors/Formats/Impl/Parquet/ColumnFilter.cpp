#include "ColumnFilter.h"
#include <base/Decimal.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/Set.h>
#include <Processors/Formats/Impl/Parquet/xsimd_wrapper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
}

template<class T>
struct PhysicTypeTraits
{
    using simd_internal_type = T;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};
template struct PhysicTypeTraits<Int32>;
template struct PhysicTypeTraits<Int64>;
template<> struct PhysicTypeTraits<Float32>
{
    using simd_internal_type = Float32;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<Int32>;
};
template<> struct PhysicTypeTraits<Float64>
{
    using simd_internal_type = Float64;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<Int64>;
};
template<> struct PhysicTypeTraits<DateTime64>
{
    using simd_internal_type = Int64;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};

template <typename T, typename S>
void FilterHelper::filterPlainFixedData(const S* src, PaddedPODArray<T> & dst, const RowSet & row_set, size_t rows_to_read)
{
    using batch_type = PhysicTypeTraits<S>::simd_type;
    using bool_type = PhysicTypeTraits<S>::simd_bool_type;
    auto increment = batch_type::size;
    auto num_batched = rows_to_read / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto rows = i * increment;
        bool_type mask = bool_type::load_aligned(row_set.maskReference().data() + rows);
        if (xsimd::none(mask))
            continue;
        else if (xsimd::all(mask))
        {
            auto old_size = dst.size();
            dst.resize( old_size + increment);
            auto * start = dst.data() + old_size;
            batch_type data = batch_type::load_unaligned(src + rows);
            if constexpr (std::is_same_v<T, S>)
                data.store_unaligned(start);
            else
            {
                alignas(xsimd::default_arch::alignment()) S buffer[increment];
                data.store_aligned(buffer);
                for (size_t j = 0; j < increment; ++j)
                    dst.push_back(static_cast<T>(buffer[j]));
            }
        }
        else
        {
            for (size_t j = 0; j < increment; ++j)
                if (row_set.get(rows + j))
                    dst.push_back(static_cast<T>(src[rows + j]));
        }
    }
    for (size_t i = num_batched * increment; i < rows_to_read; ++i)
    {
        if (row_set.get(i))
            dst.push_back(static_cast<T>(src[i]));
    }
}

template void FilterHelper::filterPlainFixedData<Int16, Int32>(Int32 const*, DB::PaddedPODArray<Int16>&, DB::RowSet const&, size_t);
template void FilterHelper::filterPlainFixedData<Int32, Int32>(Int32 const*, DB::PaddedPODArray<Int32>&, DB::RowSet const&, size_t);
template void FilterHelper::filterPlainFixedData<Int64, Int64>(const Int64* src, PaddedPODArray<Int64> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<Float32, Float32>(const Float32* src, PaddedPODArray<Float32> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<Float64, Float64>(const Float64* src, PaddedPODArray<Float64> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<DateTime64, Int64>(const Int64* src, PaddedPODArray<DateTime64> & dst, const RowSet & row_set, size_t rows_to_read);

template <typename T>
void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<T> & dict, PaddedPODArray<T> & dst, const PaddedPODArray<Int32> & idx, size_t rows_to_read)
{
    using batch_type = PhysicTypeTraits<T>::simd_type;
    using idx_batch_type = PhysicTypeTraits<T>::simd_idx_type;
    using simd_internal_type = PhysicTypeTraits<T>::simd_internal_type;
    auto increment = batch_type::size;
    auto num_batched = rows_to_read / increment;
    dst.resize(dst.size() + (num_batched * increment));
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto rows = i * increment;
        idx_batch_type idx_batch = idx_batch_type::load_unaligned(idx.data() + rows);
        batch_type::gather(reinterpret_cast<const simd_internal_type *>(dict.data()), idx_batch).store_aligned(reinterpret_cast<simd_internal_type *>(dst.data() + rows));
    }
    for (size_t i = num_batched * increment; i < rows_to_read; ++i)
    {
        dst.push_back(dict[idx[i]]);
    }
}

template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<Int32> & dict, PaddedPODArray<Int32> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<Int64> & dict, PaddedPODArray<Int64> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<Float32> & dict, PaddedPODArray<Float32> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<Float64> & dict, PaddedPODArray<Float64> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<DateTime64> & dict, PaddedPODArray<DateTime64> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
template void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<Int16> & dict, PaddedPODArray<Int16> & data, const PaddedPODArray<Int32> & idx, size_t rows_to_read);

template <typename T>
void FilterHelper::filterDictFixedData(const PaddedPODArray<T> & dict, PaddedPODArray<T> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read)
{
    using batch_type = PhysicTypeTraits<T>::simd_type;
    using bool_type = PhysicTypeTraits<T>::simd_bool_type;
    using idx_batch_type = PhysicTypeTraits<T>::simd_idx_type;
    using simd_internal_type = PhysicTypeTraits<T>::simd_internal_type;
    auto increment = batch_type::size;
    auto num_batched = rows_to_read / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto rows = i * increment;
        bool_type mask = bool_type::load_aligned(row_set.maskReference().data() + rows);
        if (xsimd::none(mask))
            continue;
        else if (xsimd::all(mask))
        {
            auto old_size = dst.size();
            auto * start = dst.data() + old_size;
            dst.resize( old_size + increment);
            idx_batch_type idx_batch = idx_batch_type::load_unaligned(idx.data() + rows);
            auto batch = batch_type::gather(reinterpret_cast<const simd_internal_type *>(dict.data()), idx_batch);
            batch.store_unaligned(reinterpret_cast<simd_internal_type *>(start));
        }
        else
        {
            for (size_t j = 0; j < increment; ++j)
                if (row_set.get(rows + j))
                    dst.push_back(dict[idx[rows + j]]);
        }
    }
    for (size_t i = num_batched * increment; i < rows_to_read; ++i)
    {
        if (row_set.get(i))
            dst.push_back(dict[idx[i]]);
    }
}

template void FilterHelper::filterDictFixedData(const PaddedPODArray<Int32> & dict, PaddedPODArray<Int32> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterDictFixedData(const PaddedPODArray<Int64> & dict, PaddedPODArray<Int64> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterDictFixedData(const PaddedPODArray<Float32> & dict, PaddedPODArray<Float32> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterDictFixedData(const PaddedPODArray<Float64> & dict, PaddedPODArray<Float64> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterDictFixedData(const PaddedPODArray<DateTime64> & dict, PaddedPODArray<DateTime64> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterDictFixedData(const PaddedPODArray<Int16> & dict, PaddedPODArray<Int16> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read);


template<class T>
void Int64RangeFilter::testIntValues(RowSet & row_set, size_t offset, size_t len, const T * data) const
{
    using batch_type = xsimd::batch<T>;
    using bool_type = xsimd::batch_bool<T>;
    auto increment = batch_type::size;
    auto num_batched = len / increment;
    batch_type min_batch = batch_type::broadcast(min);
    batch_type max_batch;
    if (!is_single_value)
        max_batch = batch_type::broadcast(max);
    bool aligned = offset % increment == 0;
    for (size_t i = 0; i < num_batched; ++i)
    {
        batch_type value;
        const auto rows = offset + (i * increment);
        if (aligned)
            value = batch_type::load_aligned(data + rows);
        else
            value = batch_type::load_unaligned(data + rows);
        bool_type mask;
        if (is_single_value)
        {
            if constexpr (std::is_same_v<T, Int32>)
            {
                if unlikely(lower32 != min)
                    mask = bool_type(false);
                else
                    mask = value == min_batch;
            }
            else if constexpr (std::is_same_v<T, Int16>)
            {
                if unlikely(lower16 != min)
                    mask = bool_type(false);
                else
                    mask = value == min_batch;
            }
            else
            {
                mask = value == min_batch;
            }
        }
        else
            mask = (value >= min_batch) && (value <= max_batch);
        if (aligned)
            mask.store_aligned(row_set.maskReference().data() + rows);
        else
            mask.store_unaligned(row_set.maskReference().data() + rows);
    }
    for (size_t i = offset + (num_batched * increment); i < offset + len ; ++i)
    {
        row_set.maskReference()[i] = data[i] >= min & data[i] <= max;
    }
}

template void Int64RangeFilter::testIntValues(RowSet & row_set, size_t offset, size_t len, const Int64 * data) const;
template void Int64RangeFilter::testIntValues(RowSet & row_set, size_t offset, size_t len, const Int32 * data) const;
template void Int64RangeFilter::testIntValues(RowSet & row_set, size_t offset, size_t len, const Int16 * data) const;

void Int64RangeFilter::testInt64Values(DB::RowSet & row_set, size_t offset, size_t len, const Int64 * data) const
{
    testIntValues(row_set, offset, len, data);
}

void Int64RangeFilter::testInt32Values(RowSet & row_set, size_t offset, size_t len, const Int32 * data) const
{
    testIntValues(row_set, offset, len, data);
}
void Int64RangeFilter::testInt16Values(RowSet & row_set, size_t offset, size_t len, const Int16 * data) const
{
    testIntValues(row_set, offset, len, data);
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

OptionalFilter Int64RangeFilter::create(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return std::nullopt;
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    if (!isInt64(input_node->result_type) && !isInt32(input_node->result_type) && !isInt16(input_node->result_type))
        return std::nullopt;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    Int64 value = constant_nodes.front()->column->getInt(0);
    ColumnFilterPtr filter = nullptr;
    if (func_name == "equals")
    {
        filter = std::make_shared<Int64RangeFilter>(value, value, false);
    }
    else if (func_name == "less")
    {
        filter = std::make_shared<Int64RangeFilter>(std::numeric_limits<Int64>::min(), value - 1, false);
    }
    else if (func_name == "greater")
    {
        filter = std::make_shared<Int64RangeFilter>(value + 1, std::numeric_limits<Int64>::max(), false);
    }
    else if (func_name == "lessOrEquals")
    {
        filter = std::make_shared<Int64RangeFilter>(std::numeric_limits<Int64>::min(), value, true);
    }
    else if (func_name == "greaterOrEquals")
    {
        filter = std::make_shared<Int64RangeFilter>(value, std::numeric_limits<Int64>::max(), true);
    }
    if (filter)
    {
        return std::make_optional(std::make_pair(name, filter));
    }
    return std::nullopt;
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
                std::max(min, other_range->min), std::min(max, other_range->max), both_null_allowed);
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
            return clone(false);
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
OptionalFilter ByteValuesFilter::create(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return std::nullopt;
    const auto * input_node = getInputNode(node);
    auto name = input_node->result_name;
    if (!isString(input_node->result_type))
        return std::nullopt;
    auto constant_nodes = getConstantNode(node);
    auto func_name = node.function_base->getName();
    ColumnFilterPtr filter = nullptr;
    if (func_name == "equals")
    {
        auto value = constant_nodes.front()->column->getDataAt(0);
        String str;
        str.resize(value.size);
        memcpy(str.data(), value.data, value.size);
        std::vector<String> values = {str};
        filter = std::make_shared<ByteValuesFilter>(values, false);
    }
    else if (func_name == "in")
    {
        const auto *arg = checkAndGetColumn<const ColumnConst>(constant_nodes.front()->column.get());
        const auto * column_set = checkAndGetColumn<const ColumnSet>(&arg->getDataColumn());
        if (!column_set)
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Only ColumnSet is supported in IN clause, but got {}", arg->getDataColumn().getName());
        auto set = column_set->getData()->get();
        auto elements = set->getSetElements().front();
        std::vector<String> values;
        for (size_t i = 0; i < elements->size(); ++i)
        {
            auto value = elements->getDataAt(i);
            String str;
            str.resize(value.size);
            memcpy(str.data(), value.data, value.size);
            values.emplace_back(str);
        }
        filter = std::make_shared<ByteValuesFilter>(values, false);
    }
    if (filter)
    {
        return std::make_optional(std::make_pair(name, filter));
    }
    return std::nullopt;
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
                lower_unbounded && otherRange->lower_unbounded;
            auto both_upper_unbounded =
                upper_unbounded && otherRange->upper_unbounded;

            auto lower_exclusive_ = !both_lower_unbounded &&
                (!testFloat64(lower) || !other->testFloat64(lower));
            auto upper_exclusive_ = !both_upper_unbounded &&
                (!testFloat64(upper) || !other->testFloat64(upper));

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

template <> class FloatRangeFilter<Float32>;
template <> class FloatRangeFilter<Float64>;

OptionalFilter createFloatRangeFilter(const ActionsDAG::Node & node)
{
    if (!isCompareColumnWithConst(node))
        return std::nullopt;
    const auto * input_node = getInputNode(node);
    if (!isFloat(input_node->result_type))
        return std::nullopt;

    bool is_float32 = WhichDataType(input_node->result_type).isFloat32();
    if (is_float32)
    {
        return Float32RangeFilter::create(node);
    }
    else
    {
        return Float64RangeFilter::create(node);
    }
}

bool RowSet::none() const
{
    bool res = true;
    auto increment = xsimd::batch_bool<unsigned char>::size;
    auto num_batched = max_rows / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto batch = xsimd::batch_bool<unsigned char>::load_aligned(mask.data() + (i * increment));
        res &= xsimd::none(batch);
        if (!res)
            return false;
    }
    for (size_t i = num_batched * increment; i < max_rows; ++i)
    {
        res &= !mask[i];
    }
    return res;
}
bool RowSet::all() const
{
    bool res = true;
    auto increment = xsimd::batch_bool<unsigned char>::size;
    auto num_batched = max_rows / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto batch = xsimd::batch_bool<unsigned char>::load_aligned(mask.data() + (i * increment));
        res &= xsimd::all(batch);
        if (!res)
            return res;
    }
    for (size_t i = num_batched * increment; i < max_rows; ++i)
    {
        res &= mask[i];
    }
    return res;
}

bool RowSet::any() const
{
    bool res = false;
    auto increment = xsimd::batch_bool<unsigned char>::size;
    auto num_batched = max_rows / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto batch = xsimd::batch_bool<unsigned char>::load_aligned(mask.data() + (i * increment));
        res |= xsimd::any(batch);
    }
    for (size_t i = num_batched * increment; i < max_rows; ++i)
    {
        res |= mask[i];
    }
    return res;
}
}
