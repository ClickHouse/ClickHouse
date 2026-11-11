#include <Columns/replaceNegativeZeros.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <bit>


namespace DB
{

namespace
{

/// The check is written on the binary representation: it vectorizes, and negative zero has exactly one.
template <typename T>
auto bitsOf(T x)
{
    if constexpr (std::is_same_v<T, BFloat16>)
        return x.raw();
    else if constexpr (sizeof(T) == sizeof(UInt32))
        return std::bit_cast<UInt32>(x);
    else
        return std::bit_cast<UInt64>(x);
}

template <typename T>
bool isNegativeZero(T x)
{
    using Bits = decltype(bitsOf(x));
    return bitsOf(x) == Bits(Bits(1) << (sizeof(Bits) * 8 - 1));
}

template <typename T>
bool containsNegativeZero(const PaddedPODArray<T> & data)
{
    /// Branchless inside a block, so that the loop is vectorized, with an early exit between the blocks.
    static constexpr size_t block_size = 256;
    const size_t size = data.size();
    for (size_t begin = 0; begin < size; begin += block_size)
    {
        const size_t end = std::min(size, begin + block_size);
        bool found = false;
        for (size_t i = begin; i < end; ++i)
            found |= isNegativeZero(data[i]);
        if (found)
            return true;
    }
    return false;
}

template <typename T>
ColumnPtr replaceInVector(const ColumnVector<T> & column)
{
    const auto & data = column.getData();
    if (!containsNegativeZero(data))
        return nullptr;

    auto res = ColumnVector<T>::create(data.size());
    auto & res_data = res->getData();
    for (size_t i = 0; i < data.size(); ++i)
        res_data[i] = isNegativeZero(data[i]) ? T{} : data[i];
    return res;
}

}


ColumnPtr replaceNegativeZeros(const IColumn & column)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column))
        return replaceInVector(*column_float64);

    if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column))
        return replaceInVector(*column_float32);

    if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
        return replaceInVector(*column_bfloat16);

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (auto nested = replaceNegativeZeros(nullable->getNestedColumn()))
            return ColumnNullable::create(nested, nullable->getNullMapColumnPtr());
        return nullptr;
    }

    if (const auto * array = typeid_cast<const ColumnArray *>(&column))
    {
        if (auto data = replaceNegativeZeros(array->getData()))
            return ColumnArray::create(data, array->getOffsetsPtr());
        return nullptr;
    }

    if (const auto * map = typeid_cast<const ColumnMap *>(&column))
    {
        if (auto nested = replaceNegativeZeros(map->getNestedColumn()))
            return ColumnMap::create(nested);
        return nullptr;
    }

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        Columns elements;
        for (size_t i = 0; i < tuple->tupleSize(); ++i)
        {
            if (auto element = replaceNegativeZeros(tuple->getColumn(i)))
            {
                if (elements.empty())
                    elements = tuple->getColumnsCopy();
                elements[i] = std::move(element);
            }
        }
        if (elements.empty())
            return nullptr;
        return ColumnTuple::create(elements);
    }

    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        /// The dictionary can hold both zeros as different values, so it cannot be modified in place:
        /// the column is rebuilt with a new dictionary.
        if (!replaceNegativeZeros(*low_cardinality->getDictionary().getNestedNotNullableColumn()))
            return nullptr;

        auto full = low_cardinality->convertToFullColumn();
        auto res = low_cardinality->cloneEmpty();
        assert_cast<ColumnLowCardinality &>(*res).insertRangeFromFullColumn(*replaceNegativeZeros(full), 0, full->size());
        return res;
    }

    if (const auto * sparse = typeid_cast<const ColumnSparse *>(&column))
    {
        if (auto values = replaceNegativeZeros(sparse->getValuesColumn()))
            return ColumnSparse::create(values, sparse->getOffsetsPtr(), sparse->size());
        return nullptr;
    }

    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
    {
        if (auto data = replaceNegativeZeros(column_const->getDataColumn()))
            return ColumnConst::create(data, column_const->size());
        return nullptr;
    }

    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(&column))
    {
        if (auto nested = replaceNegativeZeros(*replicated->getNestedColumn()))
            return ColumnReplicated::create(nested, replicated->getIndexesColumn());
        return nullptr;
    }

    return nullptr;
}

bool containsFloatingPointZero(const IColumn & column)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column))
        return std::ranges::any_of(column_float64->getData(), [](Float64 x) { return x == 0; });

    if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column))
        return std::ranges::any_of(column_float32->getData(), [](Float32 x) { return x == 0; });

    if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
        return std::ranges::any_of(column_bfloat16->getData(), [](BFloat16 x) { return x == BFloat16(0.0f); });

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return containsFloatingPointZero(nullable->getNestedColumn());

    if (const auto * array = typeid_cast<const ColumnArray *>(&column))
        return containsFloatingPointZero(array->getData());

    if (const auto * map = typeid_cast<const ColumnMap *>(&column))
        return containsFloatingPointZero(map->getNestedColumn());

    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        for (size_t i = 0; i < tuple->tupleSize(); ++i)
            if (containsFloatingPointZero(tuple->getColumn(i)))
                return true;
        return false;
    }

    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
        return containsFloatingPointZero(*low_cardinality->getDictionary().getNestedNotNullableColumn());

    if (const auto * sparse = typeid_cast<const ColumnSparse *>(&column))
        return containsFloatingPointZero(sparse->getValuesColumn());

    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
        return containsFloatingPointZero(column_const->getDataColumn());

    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(&column))
        return containsFloatingPointZero(*replicated->getNestedColumn());

    return false;
}

ColumnPtr replaceNegativeZeros(const ColumnPtr & column)
{
    if (auto res = replaceNegativeZeros(*column))
        return res;
    return column;
}

void replaceNegativeZeros(ColumnRawPtrs & key_columns, Columns & holder)
{
    for (auto & key_column : key_columns)
    {
        if (auto res = replaceNegativeZeros(*key_column))
        {
            holder.push_back(std::move(res));
            key_column = holder.back().get();
        }
    }
}

}
