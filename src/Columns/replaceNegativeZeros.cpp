#include <Columns/replaceNegativeZeros.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
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
ColumnPtr replaceInVector(const PaddedPODArray<T> & data)
{
    auto res = ColumnVector<T>::create(data.size());
    auto & res_data = res->getData();
    for (size_t i = 0; i < data.size(); ++i)
        res_data[i] = isNegativeZero(data[i]) ? T{} : data[i];
    return res;
}

bool hasNegativeZeros(const IColumn & column)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column))
        return containsNegativeZero(column_float64->getData());
    if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column))
        return containsNegativeZero(column_float32->getData());
    if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
        return containsNegativeZero(column_bfloat16->getData());

    /// `forEachSubcolumn` skips a shared dictionary.
    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
        return hasNegativeZeros(*low_cardinality->getDictionary().getNestedNotNullableColumn());

    /// A value of `Dynamic` is stored either in a typed variant or serialized in the shared variant, depending on
    /// the order of inserts, and both layouts give the value the same key. Only the typed one could be modified,
    /// so the same `-0.` would get two keys. `Dynamic` is not modified at all, also as a path of `JSON`.
    if (typeid_cast<const ColumnDynamic *>(&column))
        return false;

    bool found = false;
    column.forEachSubcolumn([&](const auto & subcolumn) { found = found || hasNegativeZeros(*subcolumn); });
    return found;
}

/// The column has to contain negative zeros.
ColumnPtr replaceNegativeZerosImpl(const ColumnPtr & column)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(column.get()))
        return replaceInVector(column_float64->getData());
    if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(column.get()))
        return replaceInVector(column_float32->getData());
    if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(column.get()))
        return replaceInVector(column_bfloat16->getData());

    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        /// The dictionary can hold both zeros as different values, so it cannot be modified in place:
        /// the column is rebuilt with a new dictionary.
        auto full = replaceNegativeZerosImpl(low_cardinality->convertToFullColumn());
        auto res = low_cardinality->cloneEmpty();
        assert_cast<ColumnLowCardinality &>(*res).insertRangeFromFullColumn(*full, 0, full->size());
        return res;
    }

    /// The source column is still referenced, so this is a copy. It is only made when there are negative zeros.
    auto res = IColumn::mutate(column);
    res->forEachMutableSubcolumn([](auto & subcolumn)
    {
        if (hasNegativeZeros(*subcolumn))
            subcolumn = replaceNegativeZerosImpl(subcolumn);
    });
    return res;
}

}


ColumnPtr replaceNegativeZeros(const IColumn & column)
{
    if (!hasNegativeZeros(column))
        return nullptr;
    return replaceNegativeZerosImpl(column.getPtr());
}

ColumnPtr replaceNegativeZeros(const ColumnPtr & column)
{
    if (!hasNegativeZeros(*column))
        return column;
    return replaceNegativeZerosImpl(column);
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
