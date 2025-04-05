#include <format>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Set.h>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <Processors/Formats/Impl/Parquet/xsimd_wrapper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

template <class T>
struct PhysicTypeTraits
{
    using simd_internal_type = T;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};
template <>
struct PhysicTypeTraits<Int8>
{
    using simd_internal_type = int8_t;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};
template <>
struct PhysicTypeTraits<char8_t>
{
    using simd_internal_type = int8_t;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};
template struct PhysicTypeTraits<Int32>;
template struct PhysicTypeTraits<Int64>;

template <>
struct PhysicTypeTraits<Float32>
{
    using simd_internal_type = Float32;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<Int32>;
};
template <>
struct PhysicTypeTraits<Float64>
{
    using simd_internal_type = Float64;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<Int64>;
};
template <>
struct PhysicTypeTraits<DateTime64>
{
    using simd_internal_type = Int64;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};

template <>
struct PhysicTypeTraits<Decimal32>
{
    using simd_internal_type = Int32;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};

template <>
struct PhysicTypeTraits<Decimal64>
{
    using simd_internal_type = Int64;
    using simd_type = xsimd::batch<simd_internal_type>;
    using simd_bool_type = xsimd::batch_bool<simd_internal_type>;
    using simd_idx_type = xsimd::batch<simd_internal_type>;
};

template <typename T, typename S>
void FilterHelper::filterPlainFixedData(const S * src, PaddedPODArray<T> & dst, const RowSet & row_set, size_t rows_to_read)
{
    using batch_type = typename PhysicTypeTraits<S>::simd_type;
    using bool_type = typename PhysicTypeTraits<S>::simd_bool_type;
    auto increment = batch_type::size;
    auto num_batched = rows_to_read / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto rows = i * increment;
        bool_type mask = bool_type::load_aligned(row_set.activeAddress() + rows);
        auto old_size = dst.size();
        if (xsimd::none(mask))
            continue;
        else if (xsimd::all(mask))
        {
            dst.resize(old_size + increment);
            if constexpr (std::is_same_v<T, S>)
            {
                auto * start = dst.data() + old_size;
                memcpySmallAllowReadWriteOverflow15(start, src + rows, increment * sizeof(S));
            }
            else
            {
                for (size_t j = 0; j < increment; ++j)
                {
                    dst[old_size + j] = static_cast<T>(src[rows + j]);
                }
            }
        }
        else
        {
            for (size_t j = 0; j < increment; ++j)
            {
                size_t idx = rows + j;
                if (row_set.get(idx))
                    dst.push_back(static_cast<T>(src[idx]));
            }
        }
    }
    for (size_t i = num_batched * increment; i < rows_to_read; ++i)
    {
        if (row_set.get(i))
            dst.push_back(static_cast<T>(src[i]));
    }
}

template void
FilterHelper::filterPlainFixedData<char8_t, char8_t>(char8_t const *, DB::PaddedPODArray<char8_t> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int8, Int8>(Int8 const *, DB::PaddedPODArray<Int8> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<int8_t, int8_t>(int8_t const *, DB::PaddedPODArray<int8_t> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt8, Int32>(Int32 const *, DB::PaddedPODArray<UInt8> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int8, Int32>(Int32 const *, DB::PaddedPODArray<Int8> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int16, Int32>(Int32 const *, DB::PaddedPODArray<Int16> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int16, Int16>(Int16 const *, DB::PaddedPODArray<Int16> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt16, Int32>(Int32 const *, DB::PaddedPODArray<UInt16> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt16, UInt16>(UInt16 const *, DB::PaddedPODArray<UInt16> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int32, Int32>(Int32 const *, DB::PaddedPODArray<Int32> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt32, UInt32>(UInt32 const *, DB::PaddedPODArray<UInt32> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt32, Int32>(Int32 const *, DB::PaddedPODArray<UInt32> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt32, Int64>(Int64 const *, DB::PaddedPODArray<UInt32> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt64, Int64>(Int64 const *, DB::PaddedPODArray<UInt64> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<UInt64, UInt64>(UInt64 const *, DB::PaddedPODArray<UInt64> &, DB::RowSet const &, size_t);
template void FilterHelper::filterPlainFixedData<Int64, Int64>(
    const Int64 * src, PaddedPODArray<Int64> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<Float32, Float32>(
    const Float32 * src, PaddedPODArray<Float32> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<Float64, Float64>(
    const Float64 * src, PaddedPODArray<Float64> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<DateTime64, Int64>(
    const Int64 * src, PaddedPODArray<DateTime64> & dst, const RowSet & row_set, size_t rows_to_read);
template void FilterHelper::filterPlainFixedData<DateTime64, DateTime64>(
    const DateTime64 * src, PaddedPODArray<DateTime64> & dst, const RowSet & row_set, size_t rows_to_read);
template void
FilterHelper::filterPlainFixedData<Decimal32, Decimal32>(Decimal32 const *, DB::PaddedPODArray<Decimal32> &, DB::RowSet const &, size_t);
template void
FilterHelper::filterPlainFixedData<Decimal64, Decimal64>(Decimal64 const *, DB::PaddedPODArray<Decimal64> &, DB::RowSet const &, size_t);
template void
FilterHelper::filterPlainFixedData<Decimal32, Int32>(Int32 const *, DB::PaddedPODArray<Decimal32> &, DB::RowSet const &, size_t);
template void
FilterHelper::filterPlainFixedData<Decimal64, Int64>(Int64 const *, DB::PaddedPODArray<Decimal64> &, DB::RowSet const &, size_t);

template <typename T>
void FilterHelper::gatherDictFixedValue(
    const PaddedPODArray<T> & dict, PaddedPODArray<T> & dst, const PaddedPODArray<Int32> & idx, size_t rows_to_read)
{
    dst.resize(rows_to_read);
    for (size_t i = 0; i < rows_to_read; ++i)
    {
        dst[i] = dict[idx[i]];
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
void FilterHelper::filterDictFixedData(
    const PaddedPODArray<T> & dict, PaddedPODArray<T> & dst, const PaddedPODArray<Int32> & idx, const RowSet & row_set, size_t rows_to_read)
{
    using batch_type = typename PhysicTypeTraits<T>::simd_type;
    using bool_type = typename PhysicTypeTraits<T>::simd_bool_type;
    using idx_batch_type = typename PhysicTypeTraits<T>::simd_idx_type;
    using simd_internal_type = typename PhysicTypeTraits<T>::simd_internal_type;
    auto increment = batch_type::size;
    auto num_batched = rows_to_read / increment;
    for (size_t i = 0; i < num_batched; ++i)
    {
        auto rows = i * increment;
        bool_type mask = bool_type::load_aligned(row_set.activeAddress() + rows);
        if (xsimd::none(mask))
            continue;
        else if (xsimd::all(mask))
        {
            auto old_size = dst.size();
            auto * start = dst.data() + old_size;
            dst.resize(old_size + increment);
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

template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<Int32> & dict,
    PaddedPODArray<Int32> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);
template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<Int64> & dict,
    PaddedPODArray<Int64> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);
template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<Float32> & dict,
    PaddedPODArray<Float32> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);
template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<Float64> & dict,
    PaddedPODArray<Float64> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);
template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<DateTime64> & dict,
    PaddedPODArray<DateTime64> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);
template void FilterHelper::filterDictFixedData(
    const PaddedPODArray<Int16> & dict,
    PaddedPODArray<Int16> & dst,
    const PaddedPODArray<Int32> & idx,
    const RowSet & row_set,
    size_t rows_to_read);


template <class T, bool negated>
void BigIntRangeFilter::testIntValues(RowSet & row_set, size_t len, const T * data) const
{
    using batch_type = xsimd::batch<T>;
    using bool_type = xsimd::batch_bool<T>;
    auto increment = batch_type::size;
    auto num_batched = len / increment;
    batch_type min_batch = batch_type::broadcast(lower);
    batch_type max_batch;
    if (!is_single_value)
    {
        if constexpr (std::is_same_v<T, Int64>)
            max_batch = batch_type::broadcast(upper);
        else if constexpr (std::is_same_v<T, Int32>)
            max_batch = batch_type::broadcast(upper32);
        else if constexpr (std::is_same_v<T, Int16>)
            max_batch = batch_type::broadcast(upper16);
        else
            UNREACHABLE();
    }

    bool aligned = (reinterpret_cast<int64_t>(data) % batch_type::arch_type::alignment() == 0) && (row_set.getOffset() % increment == 0);
    for (size_t i = 0; i < num_batched; ++i)
    {
        batch_type value;
        const auto rows = i * increment;
        if (aligned)
            value = batch_type::load_aligned(data + rows);
        else
            value = batch_type::load_unaligned(data + rows);
        bool_type mask;
        if (is_single_value)
        {
            if constexpr (std::is_same_v<T, Int32>)
            {
                if unlikely (lower32 != lower)
                    mask = bool_type(false);
                else
                    mask = value == min_batch;
            }
            else if constexpr (std::is_same_v<T, Int16>)
            {
                if unlikely (lower16 != lower)
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
        if constexpr (negated)
            mask = ~mask;
        if (aligned)
            mask.store_aligned(row_set.activeAddress() + rows);
        else
            mask.store_unaligned(row_set.activeAddress() + rows);
    }
    for (size_t i = num_batched * increment; i < len; ++i)
    {
        bool value = data[i] >= lower & data[i] <= upper;
        if (negated)
            value = !value;
        row_set.set(i, value);
    }
}

template void BigIntRangeFilter::testIntValues<Int64, true>(RowSet & row_set, size_t len, const Int64 * data) const;
template void BigIntRangeFilter::testIntValues<Int32, true>(RowSet & row_set, size_t len, const Int32 * data) const;
template void BigIntRangeFilter::testIntValues<Int16, true>(RowSet & row_set, size_t len, const Int16 * data) const;

void BigIntRangeFilter::testInt64Values(DB::RowSet & row_set, size_t len, const Int64 * data) const
{
    testIntValues(row_set, len, data);
}

void BigIntRangeFilter::testInt32Values(RowSet & row_set, size_t len, const Int32 * data) const
{
    testIntValues(row_set, len, data);
}
void BigIntRangeFilter::testInt16Values(RowSet & row_set, size_t len, const Int16 * data) const
{
    testIntValues(row_set, len, data);
}

static bool isFunctionNode(const ActionsDAG::Node & node)
{
    return node.function_base != nullptr;
}

static bool isInputNode(const ActionsDAG::Node & node)
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

namespace
{
ColumnFilterPtr nullOrFalse(bool null_allowed)
{
    if (null_allowed)
    {
        return std::make_shared<IsNullFilter>();
    }
    return std::make_shared<AlwaysFalseFilter>();
}

ColumnFilterPtr notNullOrTrue(bool nullAllowed)
{
    if (nullAllowed)
    {
        return std::make_shared<AlwaysTrueFilter>();
    }
    return std::make_shared<IsNotNullFilter>();
}

ColumnFilterPtr createBigIntValuesFilter(const std::vector<Int64> & values, bool nullAllowed, bool negated)
{
    if (values.empty())
    {
        if (!negated)
        {
            return nullOrFalse(nullAllowed);
        }
        return notNullOrTrue(nullAllowed);
    }
    if (values.size() == 1)
    {
        if (negated)
        {
            return std::make_shared<NegatedBigIntRangeFilter>(values.front(), values.front(), nullAllowed);
        }
        return std::make_shared<BigIntRangeFilter>(values.front(), values.front(), nullAllowed);
    }
    Int64 min = values.at(0);
    Int64 max = values.at(0);
    for (size_t i = 1; i < values.size(); ++i)
    {
        if (values.at(i) > max)
        {
            max = values.at(i);
        }
        else if (values.at(i) < min)
        {
            min = values.at(i);
        }
    }
    // If bitmap would have more than 4 words per set bit, we prefer a
    // hash table. If bitmap fits in under 32 words, we use bitmap anyhow.
    Int64 range;
    bool overflow = __builtin_sub_overflow(max, min, &range);
    if (likely(!overflow))
    {
        // all accepted/rejected values form one contiguous block
        if (static_cast<UInt64>(range) + 1 == values.size())
        {
            if (negated)
            {
                return std::make_shared<NegatedBigIntRangeFilter>(min, max, nullAllowed);
            }
            return std::make_shared<BigIntRangeFilter>(min, max, nullAllowed);
        }

        if (range < 32 * 64 || range < static_cast<Int64>(values.size()) * 4 * 64)
        {
            if (negated)
            {
                return std::make_shared<NegatedBigIntValuesUsingBitmaskFilter>(min, max, values, nullAllowed);
            }
            return std::make_shared<BigIntValuesUsingBitmaskFilter>(min, max, values, nullAllowed);
        }
    }
    if (negated)
    {
        return std::make_shared<NegatedBigIntValuesUsingHashTableFilter>(min, max, values, nullAllowed);
    }
    return std::make_shared<BigIntValuesUsingHashTableFilter>(min, max, values, nullAllowed);
}

ColumnFilterPtr createBigIntValuesFilter(const std::vector<Int64> & values, bool null_allowed)
{
    return createBigIntValuesFilter(values, null_allowed, false);
}

ColumnFilterPtr createNegatedBigIntValues(const std::vector<Int64> & values, bool nullAllowed)
{
    return createBigIntValuesFilter(values, nullAllowed, true);
}

ColumnFilterPtr combineBigIntRanges(std::vector<std::shared_ptr<BigIntRangeFilter>> ranges, bool nullAllowed)
{
    if (ranges.empty())
    {
        return nullOrFalse(nullAllowed);
    }

    if (ranges.size() == 1)
    {
        return std::make_shared<BigIntRangeFilter>(ranges.front()->getLower(), ranges.front()->getUpper(), nullAllowed);
    }

    return std::make_shared<BigIntMultiRangeFilter>(std::move(ranges), nullAllowed);
}

ColumnFilterPtr combineRangesAndNegatedValues(
    const std::vector<std::shared_ptr<BigIntRangeFilter>> & ranges, std::vector<Int64> & rejects, bool nullAllowed)
{
    std::vector<std::shared_ptr<BigIntRangeFilter>> outRanges;

    for (const auto & range : ranges)
    {
        auto it = std::lower_bound(rejects.begin(), rejects.end(), range->getLower());
        Int64 start = range->getLower();
        Int64 end;

        while (it != rejects.end())
        {
            end = *it - 1;
            if (start >= range->getLower() && end < range->getUpper())
            {
                if (start <= end)
                {
                    outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(start, end, false));
                }
                start = *it + 1;
                ++it;
            }
            else
            {
                break;
            }
        }
        end = range->getUpper();
        if (start <= end && start >= range->getLower() && end <= range->getUpper())
        {
            outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(start, end, false));
        }
    }

    return combineBigIntRanges(std::move(outRanges), nullAllowed);
}

ColumnFilterPtr combineNegatedBigIntLists(const std::vector<Int64> & first, const std::vector<Int64> & second, bool nullAllowed)
{
    std::vector<Int64> allRejected;
    allRejected.reserve(first.size() + second.size());

    auto it1 = first.begin();
    auto it2 = second.begin();

    // merge first and second lists
    while (it1 != first.end() && it2 != second.end())
    {
        Int64 lo = std::min(*it1, *it2);
        allRejected.emplace_back(lo);
        // remove duplicates
        if (lo == *it1)
        {
            ++it1;
        }
        if (lo == *it2)
        {
            ++it2;
        }
    }
    // fill in remaining values from each list
    while (it1 != first.end())
    {
        allRejected.emplace_back(*it1);
        ++it1;
    }
    while (it2 != second.end())
    {
        allRejected.emplace_back(*it2);
        ++it2;
    }
    return createNegatedBigIntValues(allRejected, nullAllowed);
}

ColumnFilterPtr combineNegatedRangeOnIntRanges(
    Int64 negatedLower, Int64 negatedUpper, const std::vector<std::shared_ptr<BigIntRangeFilter>> & ranges, bool nullAllowed)
{
    std::vector<std::shared_ptr<BigIntRangeFilter>> outRanges;
    // for a sensible set of ranges, at most one creates 2 output ranges
    outRanges.reserve(ranges.size() + 1);
    for (const auto & range : ranges)
    {
        if (negatedUpper < range->getLower() || range->getUpper() < negatedLower)
        {
            outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(range->getLower(), range->getUpper(), false));
        }
        else
        {
            if (range->getLower() < negatedLower)
            {
                outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(range->getLower(), negatedLower - 1, false));
            }
            if (negatedUpper < range->getUpper())
            {
                outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(negatedUpper + 1, range->getUpper(), false));
            }
        }
    }

    return combineBigIntRanges(std::move(outRanges), nullAllowed);
}

std::vector<std::shared_ptr<BigIntRangeFilter>> negatedValuesToRanges(std::vector<Int64> & values)
{
    chassert(std::is_sorted(values.begin(), values.end()));
    auto front = ++(values.begin());
    auto back = values.begin();
    std::vector<std::shared_ptr<BigIntRangeFilter>> res;
    res.reserve(values.size() + 1);
    if (*back > std::numeric_limits<Int64>::min())
    {
        res.emplace_back(std::make_shared<BigIntRangeFilter>(std::numeric_limits<Int64>::min(), *back - 1, false));
    }
    while (front != values.end())
    {
        if (*back + 1 <= *front - 1)
        {
            res.emplace_back(std::make_shared<BigIntRangeFilter>(*back + 1, *front - 1, false));
        }
        ++front;
        ++back;
    }
    if (*back < std::numeric_limits<Int64>::max())
    {
        res.emplace_back(std::make_shared<BigIntRangeFilter>(*back + 1, std::numeric_limits<Int64>::max(), false));
    }
    return res;
}

std::shared_ptr<BigIntRangeFilter> toBigIntRange(ColumnFilterPtr filter)
{
    return std::shared_ptr<BigIntRangeFilter>(dynamic_cast<BigIntRangeFilter *>(filter.get()));
}
}

ColumnFilterPtr BigIntRangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<BigIntRangeFilter>(lower, upper, false);
        case BigIntRange: {
            bool both_null_allowed = null_allowed && other->testNull();
            const auto * other_range = dynamic_cast<const BigIntRangeFilter *>(other);

            auto min = std::max(this->lower, other_range->lower);
            auto max = std::min(this->upper, other_range->upper);
            if (min < max)
                return std::make_shared<BigIntRangeFilter>(min, max, both_null_allowed);
            return nullOrFalse(both_null_allowed);
        }
        case NegatedBigIntRange:
        case BigIntValuesUsingBitmask:
        case BigIntValuesUsingHashTable:
            return other->merge(this);
        case BigIntMultiRange: {
            const auto * otherMultiRange = dynamic_cast<const BigIntMultiRangeFilter *>(other);
            std::vector<std::shared_ptr<BigIntRangeFilter>> newRanges;
            for (const auto & range : otherMultiRange->getRanges())
            {
                auto merged = this->merge(range.get());
                if (merged->kind() == BigIntRange)
                {
                    newRanges.push_back(toBigIntRange(std::move(merged)));
                }
                else
                {
                    chassert(merged->kind() == AlwaysFalse);
                }
            }

            bool bothNullAllowed = null_allowed && other->testNull();
            return combineBigIntRanges(std::move(newRanges), bothNullAllowed);
        }
        case NegatedBigIntValuesUsingBitmask:
        case NegatedBigIntValuesUsingHashTable: {
            bool bothNullAllowed = null_allowed && other->testNull();
            if (!other->testInt64Range(lower, upper, false))
            {
                return nullOrFalse(bothNullAllowed);
            }
            std::vector<Int64> vals;
            if (other->kind() == NegatedBigIntValuesUsingBitmask)
            {
                const auto * otherNegated = dynamic_cast<const NegatedBigIntValuesUsingBitmaskFilter *>(other);
                vals = otherNegated->getValues();
            }
            else
            {
                const auto * otherNegated = dynamic_cast<const NegatedBigIntValuesUsingHashTableFilter *>(other);
                vals = otherNegated->getValues();
            }
            std::vector<std::shared_ptr<BigIntRangeFilter>> rangeList;
            rangeList.emplace_back(std::make_shared<BigIntRangeFilter>(lower, upper, false));
            return combineRangesAndNegatedValues(rangeList, vals, bothNullAllowed);
        }
        default:
            return nullptr;
    }
}
ColumnFilterPtr BytesValuesFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return this->clone(std::make_optional(false));
        case BytesValues: {
            bool both_null_allowed = null_allowed && other->testNull();
            const auto * otherBytesValues = static_cast<const BytesValuesFilter *>(other);

            if (this->upper.compare(otherBytesValues->lower) < 0 || otherBytesValues->upper.compare(this->lower) < 0)
            {
                return nullOrFalse(both_null_allowed);
            }
            const BytesValuesFilter * smaller_filter = this;
            const BytesValuesFilter * larger_filter = otherBytesValues;
            if (this->getValues().size() > otherBytesValues->getValues().size())
            {
                smaller_filter = otherBytesValues;
                larger_filter = this;
            }

            std::vector<std::string> newValues;
            newValues.reserve(smaller_filter->getValues().size());

            for (const auto & value : smaller_filter->getValues())
            {
                if (larger_filter->values.contains(value))
                {
                    newValues.emplace_back(value);
                }
            }

            if (newValues.empty())
            {
                return nullOrFalse(both_null_allowed);
            }

            return std::make_shared<BytesValuesFilter>(std::move(newValues), both_null_allowed);
        }
        case NegatedBytesValues: {
            bool bothNullAllowed = null_allowed && other->testNull();
            std::vector<std::string> newValues;
            newValues.reserve(getValues().size());
            for (const auto & value : getValues())
            {
                if (other->testString(value))
                {
                    newValues.emplace_back(value);
                }
            }

            if (newValues.empty())
            {
                return nullOrFalse(bothNullAllowed);
            }

            return std::make_shared<BytesValuesFilter>(std::move(newValues), bothNullAllowed);
        }
        case BytesRange: {
            const auto * otherBytesRange = static_cast<const BytesRangeFilter *>(other);
            bool bothNullAllowed = null_allowed && other->testNull();

            if (!testStringRange(
                    otherBytesRange->isLowerUnbounded() ? std::nullopt : std::make_optional(otherBytesRange->getLower()),
                    otherBytesRange->isUpperUnbounded() ? std::nullopt : std::make_optional(otherBytesRange->getUpper()),
                    bothNullAllowed))
            {
                return nullOrFalse(bothNullAllowed);
            }

            std::vector<std::string> newValues;
            newValues.reserve(this->getValues().size());
            for (const auto & value : this->getValues())
            {
                if (otherBytesRange->testString(value))
                {
                    newValues.emplace_back(value);
                }
            }

            if (newValues.empty())
            {
                return nullOrFalse(bothNullAllowed);
            }

            return std::make_shared<BytesValuesFilter>(std::move(newValues), bothNullAllowed);
        }
        case NegatedBytesRange: {
            const auto * otherBytesRange = static_cast<const NegatedBytesRangeFilter *>(other);
            bool bothNullAllowed = null_allowed && other->testNull();

            std::vector<std::string> newValues;
            newValues.reserve(this->getValues().size());
            for (const auto & value : this->getValues())
            {
                if (otherBytesRange->testString(value))
                {
                    newValues.emplace_back(value);
                }
            }

            if (newValues.empty())
            {
                return nullOrFalse(bothNullAllowed);
            }

            return std::make_shared<BytesValuesFilter>(std::move(newValues), bothNullAllowed);
        }
        default:
            return nullptr;
    }
}

namespace
{
int compareRanges(const char * lhs, size_t length, const std::string & rhs)
{
    size_t size = std::min(length, rhs.length());
    int compare = memcmp(lhs, rhs.data(), size);
    if (compare)
    {
        return compare;
    }
    return static_cast<int>(length - rhs.size());
}
}


bool BytesValuesFilter::testStringRange(std::optional<std::string_view> min, std::optional<std::string_view> max, bool has_null) const
{
    if (has_null && null_allowed)
    {
        return true;
    }

    if (min.has_value() && max.has_value() && min.value() == max.value())
    {
        return testString(min.value());
    }

    // min > upper_
    if (min.has_value() && compareRanges(min->data(), min->length(), upper) > 0)
    {
        return false;
    }

    // max < lower_
    if (max.has_value() && compareRanges(max->data(), max->length(), lower) < 0)
    {
        return false;
    }

    return true;
}
BytesValuesFilter::~BytesValuesFilter() = default;

template <is_float T>
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
        case DoubleRange: {
            bool both_null_allowed = null_allowed && other->testNull();

            auto otherRange = static_cast<const FloatRangeFilter<T> *>(other);

            auto lower = std::max(min, otherRange->min);
            auto upper = std::min(max, otherRange->max);

            auto both_lower_unbounded = lower_unbounded && otherRange->lower_unbounded;
            auto both_upper_unbounded = upper_unbounded && otherRange->upper_unbounded;

            auto lower_exclusive_ = !both_lower_unbounded && (!testFloat64(lower) || !other->testFloat64(lower));
            auto upper_exclusive_ = !both_upper_unbounded && (!testFloat64(upper) || !other->testFloat64(upper));

            if (lower > upper || (lower == upper && lower_exclusive))
            {
                nullOrFalse(both_null_allowed);
            }
            return std::make_unique<FloatRangeFilter<T>>(
                lower, both_lower_unbounded, lower_exclusive_, upper, both_upper_unbounded, upper_exclusive_, both_null_allowed);
        }
        default:
            return nullptr;
    }
}

template class FloatRangeFilter<Float32>;
template class FloatRangeFilter<Float64>;

ColumnFilterPtr NegatedBytesValuesFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return this->clone(std::make_optional(false));
        case BytesValues:
            return other->merge(this);
        default:
            return nullptr;
    }
}


ColumnFilterPtr NegatedBigIntRangeFilter::clone(std::optional<bool> null_allowed_) const
{
    return std::make_shared<NegatedBigIntRangeFilter>(non_negated->lower, non_negated->upper, null_allowed_.value_or(null_allowed));
}

ColumnFilterPtr NegatedBigIntRangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return this->clone(std::make_optional(false));
        case BigIntRange: {
            bool bothNullAllowed = null_allowed && other->testNull();
            const auto * otherRange = static_cast<const BigIntRangeFilter *>(other);
            std::vector<std::shared_ptr<BigIntRangeFilter>> rangeList;
            rangeList.emplace_back(std::make_shared<BigIntRangeFilter>(otherRange->getLower(), otherRange->getUpper(), false));
            return combineNegatedRangeOnIntRanges(this->getLower(), this->getUpper(), rangeList, bothNullAllowed);
        }
        case NegatedBigIntRange: {
            bool bothNullAllowed = null_allowed && other->testNull();
            const auto * otherNegatedRange = static_cast<const NegatedBigIntRangeFilter *>(other);
            if (this->getLower() > otherNegatedRange->getUpper())
            {
                return other->merge(this);
            }
            chassert(this->getLower() <= otherNegatedRange->getLower());
            if (this->getUpper() + 1 < otherNegatedRange->getLower())
            {
                std::vector<std::shared_ptr<BigIntRangeFilter>> outRanges;
                Int64 smallLower = this->getLower();
                Int64 smallUpper = this->getUpper();
                Int64 bigLower = otherNegatedRange->getLower();
                Int64 bigUpper = otherNegatedRange->getLower();
                if (smallLower > std::numeric_limits<Int64>::min())
                {
                    outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(std::numeric_limits<Int64>::min(), smallLower - 1, false));
                }
                if (smallUpper < std::numeric_limits<Int64>::max() && bigLower > std::numeric_limits<Int64>::min())
                {
                    outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(smallUpper + 1, bigLower - 1, false));
                }
                if (bigUpper < std::numeric_limits<Int64>::max())
                {
                    outRanges.emplace_back(std::make_shared<BigIntRangeFilter>(bigUpper + 1, std::numeric_limits<Int64>::max(), false));
                }
                return combineBigIntRanges(std::move(outRanges), bothNullAllowed);
            }
            return std::make_shared<NegatedBigIntRangeFilter>(
                this->getLower(), std::max<Int64>(this->getUpper(), otherNegatedRange->getUpper()), bothNullAllowed);
        }
        case BigIntMultiRange: {
            bool bothNullAllowed = null_allowed && other->testNull();
            const auto *otherMultiRanges = static_cast<const BigIntMultiRangeFilter *>(other);
            return combineNegatedRangeOnIntRanges(this->getLower(), this->getUpper(), otherMultiRanges->getRanges(), bothNullAllowed);
        }
        case BigIntValuesUsingHashTable:
        case BigIntValuesUsingBitmask:
            return other->merge(this);
        case NegatedBigIntValuesUsingHashTable:
        case NegatedBigIntValuesUsingBitmask: {
            bool bothNullAllowed = null_allowed && other->testNull();
            std::vector<Int64> rejectedValues;
            if (other->kind() == NegatedBigIntValuesUsingHashTable)
            {
                const auto * otherHashTable = static_cast<const NegatedBigIntValuesUsingHashTableFilter *>(other);
                rejectedValues = otherHashTable->getValues();
            }
            else
            {
                const auto * otherBitmask = static_cast<const NegatedBigIntValuesUsingBitmaskFilter *>(other);
                rejectedValues = otherBitmask->getValues();
            }
            if (non_negated->isSingleValue())
            {
                if (other->testInt64(this->getLower()))
                {
                    rejectedValues.push_back(this->getLower());
                }
                return createNegatedBigIntValues(rejectedValues, bothNullAllowed);
            }
            return combineNegatedRangeOnIntRanges(
                this->getLower(), this->getUpper(), negatedValuesToRanges(rejectedValues), bothNullAllowed);
        }
        default:
            UNREACHABLE();
    }
}
NegatedBigIntRangeFilter::~NegatedBigIntRangeFilter() = default;

ColumnFilterPtr NegatedBytesValuesFilter::clone(std::optional<bool>) const
{
    return nullptr;
}
NegatedBytesValuesFilter::~NegatedBytesValuesFilter() = default;

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

IColumn::Filter ExpressionFilter::execute(const ColumnsWithTypeAndName & columns) const
{
    auto block = Block(columns);
    actions->execute(block);
    FilterDescription filter_desc(*block.getByName(filter_name).column);
    auto mutable_column
        = filter_desc.data_holder ? filter_desc.data_holder->assumeMutable() : block.getByName(filter_name).column->assumeMutable();
    ColumnUInt8 * uint8_col = static_cast<ColumnUInt8 *>(mutable_column.get());
    IColumn::Filter filter;
    filter.swap(uint8_col->getData());
    return filter;
}
NameSet ExpressionFilter::getInputs()
{
    NameSet result;
    auto inputs = actions->getActionsDAG().getInputs();
    for (const auto & input : inputs)
    {
        result.insert(input->result_name);
    }
    return result;
}
ExpressionFilter::ExpressionFilter(ActionsDAG && dag_)
{
    ExpressionActionsSettings settings;
    settings.can_compile_expressions = true;
    settings.min_count_to_compile_expression = 0;
    actions = std::make_shared<ExpressionActions>(std::move(dag_), settings, true);
    filter_name = actions->getActionsDAG().getOutputs().front()->result_name;
    if (!isUInt8(removeNullable(actions->getActionsDAG().getOutputs().front()->result_type)))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter result type must be UInt8");
    }
}
ColumnFilterPtr IsNullFilter::merge(const ColumnFilter * other) const
{
    if (other->testNull())
    {
        return this->clone(std::nullopt);
    }
    return std::make_shared<IsNullFilter>();
}

ColumnFilterPtr IsNotNullFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case IsNotNull:
            return this->clone(std::nullopt);
        case AlwaysFalse:
        case IsNull:
            return std::make_shared<AlwaysFalseFilter>();
        default:
            return other->merge(this);
    }
}

ColumnFilterPtr BoolValueFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<BoolValueFilter>(value, false);
        case BoolValue: {
            bool bothNullAllowed = null_allowed && other->testNull();
            if (other->testBool(value))
            {
                return std::make_shared<BoolValueFilter>(value, bothNullAllowed);
            }

            return nullOrFalse(bothNullAllowed);
        }
        default:
            return nullptr;
    }
}
BigIntValuesUsingHashTableFilter::BigIntValuesUsingHashTableFilter(
    Int64 min_, Int64 max_, const std::vector<Int64> & values_, bool nullAllowed)
    : ColumnFilter(BigIntValuesUsingHashTable, nullAllowed)
    , min(min_)
    , max(max_)
    , values(values_)
{
    constexpr int32_t kPaddingElements = 4;
    if (min >= max)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "min must be less than max");
    if (values.size() <= 1)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "values must contain at least 2 entries");

    // Size the hash table to be 2+x the entry count, e.g. 10 entries
    // gets 1 << log2 of 50 == 32. The filter is expected to fail often so we
    // wish to increase the chance of hitting empty on first probe.
    auto size = 1u << static_cast<uint32_t>(std::log2(values.size() * 5));
    hashTable.resize(size + kPaddingElements);
    sizeMask = size - 1;
    std::fill(hashTable.begin(), hashTable.end(), kEmptyMarker);
    for (auto value : values)
    {
        if (value == kEmptyMarker)
        {
            containsEmptyMarker = true;
        }
        else
        {
            auto position = ((value * M) & sizeMask);
            for (auto i = position; i < position + size; i++)
            {
                uint32_t index = i & sizeMask;
                if (hashTable[index] == kEmptyMarker)
                {
                    hashTable[index] = value;
                    break;
                }
            }
        }
    }
    // Replicate the last element of hashTable kPaddingEntries times at 'size_' so
    // that one can load a full vector of elements past the last used index.
    for (auto i = 0; i < kPaddingElements; ++i)
    {
        hashTable.at(sizeMask + 1 + i) = hashTable.at(sizeMask);
    }
    std::sort(values.begin(), values.end());
}
bool BigIntValuesUsingHashTableFilter::testInt64(Int64 value) const
{
    if (containsEmptyMarker && value == kEmptyMarker)
    {
        return true;
    }
    if (value < min || value > max)
    {
        return false;
    }
    uint32_t pos = (value * M) & sizeMask;
    for (auto i = pos; i <= pos + sizeMask; i++)
    {
        int32_t idx = i & sizeMask;
        Int64 l = hashTable.at(idx);
        if (l == kEmptyMarker)
        {
            return false;
        }
        if (l == value)
        {
            return true;
        }
    }
    return false;
}
bool BigIntValuesUsingHashTableFilter::testInt64Range(Int64 min_, Int64 max_, bool has_null) const
{
    if (has_null && null_allowed)
    {
        return true;
    }

    if (min == max)
    {
        return testInt64(min);
    }

    if (min > max_ || max < min_)
    {
        return false;
    }
    auto it = std::lower_bound(values.begin(), values.end(), min);
    chassert(it != values.end()); // min is already tested to be <= max_.
    if (min == *it)
    {
        return true;
    }
    return max >= *it;
}
BigIntValuesUsingBitmaskFilter::BigIntValuesUsingBitmaskFilter(
    Int64 min_, Int64 max_, const std::vector<Int64> & values_, bool null_allowed_)
    : ColumnFilter(BigIntValuesUsingBitmask, null_allowed_)
    , min(min_)
    , max(max_)
{
    if (min >= max)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "min must be less than max");
    if (values_.size() <= 1)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "values must contain at least 2 entries");
    bitmask.resize(max - min + 1);
    for (auto value : values_)
    {
        bitmask.at(value - min) = true;
    }
}
std::vector<Int64> BigIntValuesUsingBitmaskFilter::values() const
{
    std::vector<Int64> values;
    for (size_t i = 0; i < bitmask.size(); ++i)
    {
        if (bitmask.at(i))
            values.push_back(i + min);
    }
    return values;
}
bool BigIntValuesUsingBitmaskFilter::testInt64(Int64 value) const
{
    if (value < min || value > max)
        return false;
    return bitmask.at(value - min);
}
bool BigIntValuesUsingBitmaskFilter::testInt64Range(Int64 min_, Int64 max_, bool has_null) const
{
    if (has_null && null_allowed)
        return true;
    if (min == max)
        return testInt64(min);
    return !(min_ > max || max_ < min);
}
ColumnFilterPtr BigIntValuesUsingBitmaskFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<BigIntValuesUsingBitmaskFilter>(*this, false);
        case BigIntRange: {
            const auto * otherRange = dynamic_cast<const BigIntRangeFilter *>(other);

            auto min_ = std::max(min, otherRange->getLower());
            auto max_ = std::min(max, otherRange->getUpper());

            return merge(min_, max_, other);
        }
        case BigIntValuesUsingHashTable: {
            const auto * otherValues = dynamic_cast<const BigIntValuesUsingHashTableFilter *>(other);

            auto min_ = std::max(min, otherValues->getMin());
            auto max_ = std::min(max, otherValues->getMax());

            return merge(min_, max_, other);
        }
        case BigIntValuesUsingBitmask: {
            const auto * otherValues = dynamic_cast<const BigIntValuesUsingBitmaskFilter *>(other);

            auto min_ = std::max(min, otherValues->min);
            auto max_ = std::min(max, otherValues->max);

            return merge(min_, max_, other);
        }
        case BigIntMultiRange: {
            const auto * otherMultiRange = dynamic_cast<const BigIntMultiRangeFilter *>(other);

            std::vector<Int64> valuesToKeep;
            for (const auto & range : otherMultiRange->getRanges())
            {
                auto min_ = std::max(min, range->getLower());
                auto max_ = std::min(max, range->getUpper());
                for (auto i = min_; i <= max_; ++i)
                {
                    if (bitmask[i - min] && range->testInt64(i))
                    {
                        valuesToKeep.push_back(i);
                    }
                }
            }

            bool bothNullAllowed = null_allowed && other->testNull();
            return createBigIntValuesFilter(valuesToKeep, bothNullAllowed);
        }
        case NegatedBigIntRange:
        case NegatedBigIntValuesUsingBitmask:
        case NegatedBigIntValuesUsingHashTable: {
            return merge(min, max, other);
        }
        default:
            return nullptr;
    }
}
ColumnFilterPtr BigIntValuesUsingBitmaskFilter::merge(Int64 min_, Int64 max_, const ColumnFilter * other) const
{
    bool both_null_allowed = null_allowed && other->testNull();
    std::vector<Int64> value_to_keep;
    for (auto i = min_; i <= max_; ++i)
    {
        if (bitmask.at(i - min) && other->testInt64(i))
        {
            value_to_keep.push_back(i);
        }
    }
    return createBigIntValuesFilter(value_to_keep, both_null_allowed);
}
bool NegatedBigIntValuesUsingHashTableFilter::testInt64Range(Int64 min, Int64 max, bool has_null) const
{
    if (has_null && null_allowed)
    {
        return true;
    }

    if (min == max)
    {
        return testInt64(min);
    }

    if (max > non_negated->getMax() || min < non_negated->getMin())
    {
        return true;
    }

    auto lo = std::lower_bound(non_negated->getValues().begin(), non_negated->getValues().end(), min);
    auto hi = std::lower_bound(non_negated->getValues().begin(), non_negated->getValues().end(), max);
    chassert(lo != non_negated->getValues().end()); // min is already tested to be <= max_.
    if (min != *lo || max != *hi)
    {
        // at least one of the endpoints of the range succeeds
        return true;
    }
    // Check if all values in this range are in values_ by counting the number
    // of things between min and max
    // if distance is any less, then we are missing an element => something
    // in the range is accepted
    return (std::distance(lo, hi) != max - min);
}
ColumnFilterPtr NegatedBigIntValuesUsingHashTableFilter::merge(const ColumnFilter * other) const
{
    // Rules of NegatedBigIntValuesUsingHashTable with IsNull/IsNotNull
    // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
    // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
    // Negated...(nullAllowed=false)
    // 3. Negated...(nullAllowed=false) AND IS NULL
    // => ALWAYS FALSE
    // 4. Negated...(nullAllowed=false) AND IS NOT NULL
    // =>Negated...(nullAllowed=false)
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<NegatedBigIntValuesUsingHashTableFilter>(*this, false);
        case BigIntValuesUsingHashTable:
        case BigIntValuesUsingBitmask:
        case BigIntRange:
        case BigIntMultiRange: {
            return other->merge(this);
        }
        case NegatedBigIntValuesUsingHashTable: {
            const auto * otherNegated = static_cast<const NegatedBigIntValuesUsingHashTableFilter *>(other);
            bool both_null_allowed = null_allowed && other->testNull();
            return combineNegatedBigIntLists(getValues(), otherNegated->getValues(), both_null_allowed);
        }
        case NegatedBigIntRange:
        case NegatedBigIntValuesUsingBitmask: {
            return other->merge(this);
        }
        default:
            return nullptr;
    }
}
ColumnFilterPtr NegatedBigIntValuesUsingBitmaskFilter::merge(const ColumnFilter * other) const
{
    // Rules of NegatedBigIntValuesUsingBitmask with IsNull/IsNotNull
    // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
    // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
    // Negated...(nullAllowed=false)
    // 3. Negated...(nullAllowed=false) AND IS NULL
    // => ALWAYS FALSE
    // 4. Negated...(nullAllowed=false) AND IS NOT NULL
    // =>Negated...(nullAllowed=false)
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return std::make_shared<NegatedBigIntValuesUsingBitmaskFilter>(*this, false);
        case BigIntValuesUsingHashTable:
        case BigIntValuesUsingBitmask:
        case BigIntRange:
        case NegatedBigIntRange:
        case BigIntMultiRange: {
            return other->merge(this);
        }
        case NegatedBigIntValuesUsingHashTable: {
            const NegatedBigIntValuesUsingHashTableFilter * otherHashTable;
            otherHashTable = dynamic_cast<const NegatedBigIntValuesUsingHashTableFilter *>(other);
            bool bothNullAllowed = null_allowed && other->testNull();
            // kEmptyMarker is already in values for a bitmask
            return combineNegatedBigIntLists(getValues(), otherHashTable->getValues(), bothNullAllowed);
        }
        case NegatedBigIntValuesUsingBitmask: {
            const auto * otherBitmask = dynamic_cast<const NegatedBigIntValuesUsingBitmaskFilter *>(other);
            bool both_null_allowed = null_allowed && other->testNull();
            return combineNegatedBigIntLists(getValues(), otherBitmask->getValues(), both_null_allowed);
        }
        default:
            return nullptr;
    }
}

bool NegatedBigIntValuesUsingBitmaskFilter::testInt64Range(Int64 min_, Int64 max_, bool has_null) const
{
    if (has_null && null_allowed)
    {
        return true;
    }

    if (min_ == max_)
    {
        return testInt64(min);
    }

    return true;
}

bool BytesRangeFilter::testString(const std::string_view & value) const
{
    if (value.empty())
    {
        // Empty string. value is null. This is the smallest possible string.
        // It passes the following filters: < non-empty, <= empty | non-empty, >=
        // empty.
        if (lower_unbounded)
        {
            return !upper.empty() || !upper_exclusive;
        }

        return lower.empty() && !lower_exclusive;
    }

    if (single_value)
    {
        if (value.size() != lower.size())
        {
            return false;
        }
        return memcmp(value.data(), lower.data(), value.size()) == 0;
    }
    if (!lower_unbounded)
    {
        int compare = compareRanges(value.data(), value.size(), lower);
        if (compare < 0 || (lower_exclusive && compare == 0))
        {
            return false;
        }
    }
    if (!upper_unbounded)
    {
        int compare = compareRanges(value.data(), value.size(), upper);
        return compare < 0 || (!upper_exclusive && compare == 0);
    }
    return true;
}

bool BytesRangeFilter::testStringRange(std::optional<std::string_view> min, std::optional<std::string_view> max, bool has_null) const
{
    if (has_null && null_allowed)
    {
        return true;
    }
    return (lower_unbounded || !max.has_value() || compareRanges(max->data(), max->length(), lower) >= !!lower_exclusive)
        && (upper_unbounded || !min.has_value() || compareRanges(min->data(), min->length(), upper) <= -!!upper_exclusive);
}

namespace
{
// compareResult = left < right for upper, right < left for lower
bool mergeExclusive(int compareResult, bool left, bool right)
{
    return compareResult == 0 ? (left || right) : (compareResult < 0 ? left : right);
}
}

ColumnFilterPtr BytesRangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return this->clone(std::make_optional(false));
        case BytesValues:
        case NegatedBytesValues:
        case NegatedBytesRange:
            return other->merge(this);
        case BytesRange: {
            bool bothNullAllowed = null_allowed && other->testNull();

            const auto * otherRange = static_cast<const BytesRangeFilter *>(other);

            bool upper_unbounded_ = false;
            bool lower_unbounded_ = false;
            bool upper_exclusive_ = false;
            bool lower_exclusive_ = false;
            std::string upper_;
            std::string lower_;

            if (lower_unbounded)
            {
                lower_unbounded_ = otherRange->lower_unbounded;
                lower_exclusive_ = otherRange->lower_exclusive;
                lower_ = otherRange->lower;
            }
            else if (otherRange->lower_unbounded)
            {
                lower_unbounded_ = lower_unbounded;
                lower_exclusive_ = lower_exclusive;
                lower_ = lower;
            }
            else
            {
                lower_unbounded_ = false;
                auto compare = lower_.compare(otherRange->lower);
                lower_ = compare < 0 ? otherRange->lower : lower_;
                lower_exclusive_ = mergeExclusive(-compare, lower_exclusive, otherRange->lower_exclusive);
            }

            if (upper_unbounded)
            {
                upper_unbounded_ = otherRange->upper_unbounded;
                upper_exclusive_ = otherRange->upper_exclusive;
                upper_ = otherRange->upper;
            }
            else if (otherRange->upper_unbounded)
            {
                upper_unbounded_ = upper_unbounded;
                upper_exclusive_ = upper_exclusive;
                upper_ = upper;
            }
            else
            {
                upper_unbounded_ = false;
                auto compare = upper_.compare(otherRange->upper);
                upper_ = compare < 0 ? upper_ : otherRange->upper;
                upper_exclusive_ = mergeExclusive(compare, upper_exclusive, otherRange->upper_exclusive);
            }

            if (!lower_unbounded_ && !upper_unbounded_ && (lower > upper || (lower == upper && (lower_exclusive_ || upper_exclusive_))))
            {
                return nullOrFalse(bothNullAllowed);
            }

            return std::make_shared<BytesRangeFilter>(
                lower_, lower_unbounded_, lower_exclusive_, upper_, upper_unbounded_, upper_exclusive_, bothNullAllowed);
        }
        default:
            return nullptr;
    }
}
ColumnFilterPtr NegatedBytesRangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull:
            return this->clone(std::make_optional(false));
        case BytesValues:
            return other->merge(this);
        case NegatedBytesValues:
        case BytesRange:
        case NegatedBytesRange: {
            // doesn't support merging
            return nullptr;
        }
        default:
            return nullptr;
    }
}

BigIntMultiRangeFilter::BigIntMultiRangeFilter(std::vector<std::shared_ptr<BigIntRangeFilter>> ranges_, bool null_allowed_)
    : ColumnFilter(BigIntMultiRange, null_allowed_)
    , ranges(std::move(ranges_))
{
    if (ranges_.empty())
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "ranges is empty");
    if (ranges_.size() <= 1)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "should contain at least 2 ranges");
    for (const auto & range : ranges_)
    {
        lower_bounds.push_back(range->getLower());
    }
    for (size_t i = 1; i < lower_bounds.size(); i++)
    {
        if (lower_bounds.at(i) < ranges_.at(i - 1)->getUpper())
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "ranges overlap");
    }
}
ColumnFilterPtr BigIntMultiRangeFilter::clone(std::optional<bool> null_allowed_) const
{
    std::vector<std::shared_ptr<BigIntRangeFilter>> ranges_;
    ranges_.reserve(ranges.size());
    for (const auto & range : ranges_)
    {
        ranges_.emplace_back(range);
    }
    if (null_allowed_)
    {
        return std::make_shared<BigIntMultiRangeFilter>(std::move(ranges_), null_allowed_.value());
    }
    else
    {
        return std::make_shared<BigIntMultiRangeFilter>(std::move(ranges_), null_allowed);
    }
}
namespace
{
Int32 binarySearch(const std::vector<Int64> & values, Int64 value)
{
    auto it = std::lower_bound(values.begin(), values.end(), value);
    if (it == values.end() || *it != value)
    {
        return static_cast<Int32>(-std::distance(values.begin(), it) - 1);
    }
    else
    {
        return static_cast<Int32>(std::distance(values.begin(), it));
    }
}
}
bool BigIntMultiRangeFilter::testInt64(Int64 value) const
{
    int32_t i = binarySearch(lower_bounds, value);
    if (i >= 0)
    {
        return true;
    }
    int place = (-i) - 1;
    if (place == 0)
    {
        // Below first
        return false;
    }
    // When value did not hit a lower bound of a filter, test with the filter
    // before the place where value would be inserted.
    return ranges.at(place - 1)->testInt64(value);
}

bool BigIntMultiRangeFilter::testInt64Range(Int64 min, Int64 max, bool hasNull) const
{
    if (hasNull && null_allowed)
    {
        return true;
    }
    for (const auto & range : ranges)
    {
        if (range->testInt64Range(min, max, hasNull))
        {
            return true;
        }
    }
    return false;
}

ColumnFilterPtr BigIntMultiRangeFilter::merge(const ColumnFilter * other) const
{
    switch (other->kind())
    {
        case AlwaysTrue:
        case AlwaysFalse:
        case IsNull:
            return other->merge(this);
        case IsNotNull: {
            std::vector<std::shared_ptr<BigIntRangeFilter>> ranges_;
            ranges_.reserve(ranges.size());
            for (const auto & range : ranges)
            {
                ranges_.push_back(range);
            }
            return std::make_shared<BigIntMultiRangeFilter>(ranges, false);
        }
        case BigIntRange:
        case NegatedBigIntRange:
        case BigIntValuesUsingBitmask:
        case BigIntValuesUsingHashTable: {
            return other->merge(this);
        }
        case BigIntMultiRange: {
            std::vector<std::shared_ptr<BigIntRangeFilter>> newRanges;
            for (const auto & range : ranges)
            {
                auto merged = range->merge(other);
                if (merged->kind() == BigIntRange)
                {
                    newRanges.push_back(toBigIntRange(std::move(merged)));
                }
                else if (merged->kind() == BigIntMultiRange)
                {
                    auto *mergedMultiRange = dynamic_cast<BigIntMultiRangeFilter *>(merged.get());
                    for (const auto & newRange : mergedMultiRange->ranges)
                    {
                        newRanges.push_back(toBigIntRange(newRange));
                    }
                }
                else
                {
                    if (merged->kind() != AlwaysFalse)
                    {
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected filter type");
                    }
                }
            }

            bool bothNullAllowed = null_allowed && other->testNull();
            if (newRanges.empty())
            {
                return nullOrFalse(bothNullAllowed);
            }

            if (newRanges.size() == 1)
            {
                return std::make_shared<BigIntRangeFilter>(newRanges.front()->getLower(), newRanges.front()->getUpper(), bothNullAllowed);
            }

            return std::make_shared<BigIntMultiRangeFilter>(std::move(newRanges), bothNullAllowed);
        }
        case NegatedBigIntValuesUsingHashTable:
        case NegatedBigIntValuesUsingBitmask: {
            std::vector<std::unique_ptr<BigIntRangeFilter>> newRanges;
            std::vector<Int64> rejects;
            if (other->kind() == NegatedBigIntValuesUsingBitmask)
            {
                const auto * otherNegated = dynamic_cast<const NegatedBigIntValuesUsingBitmaskFilter *>(other);
                rejects = otherNegated->getValues();
            }
            else
            {
                const auto * otherNegated = dynamic_cast<const NegatedBigIntValuesUsingHashTableFilter *>(other);
                rejects = otherNegated->getValues();
            }

            bool bothNullAllowed = null_allowed && other->testNull();
            return combineRangesAndNegatedValues(ranges, rejects, bothNullAllowed);
        }
        default:
            UNREACHABLE();
    }
}
}
