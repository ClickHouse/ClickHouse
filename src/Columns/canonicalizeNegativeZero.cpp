#include <Columns/canonicalizeNegativeZero.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <base/normalizeNegativeZero.h>


namespace DB
{

namespace
{

template <typename T>
ColumnPtr canonicalizeNegativeZeroInVector(const IColumn & column)
{
    using Container = typename ColumnVector<T>::Container;
    const Container & data = assert_cast<const ColumnVector<T> &>(column).getData();
    const size_t size = data.size();

    /// This check is branchless and vectorizable, unlike the loop below, which is almost never executed.
    bool has_negative_zero = false;
    for (size_t i = 0; i < size; ++i)
        has_negative_zero |= isNegativeZero(data[i]);

    if (!has_negative_zero)
        return nullptr;

    auto res = ColumnVector<T>::create(size);
    Container & res_data = res->getData();
    for (size_t i = 0; i < size; ++i)
        res_data[i] = normalizeNegativeZero(data[i]);

    return res;
}

}

ColumnPtr canonicalizeNegativeZero(const IColumn & column)
{
    if (typeid_cast<const ColumnFloat64 *>(&column))
        return canonicalizeNegativeZeroInVector<Float64>(column);

    if (typeid_cast<const ColumnFloat32 *>(&column))
        return canonicalizeNegativeZeroInVector<Float32>(column);

    if (typeid_cast<const ColumnBFloat16 *>(&column))
        return canonicalizeNegativeZeroInVector<BFloat16>(column);

    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (ColumnPtr nested = canonicalizeNegativeZero(column_nullable->getNestedColumn()))
            return ColumnNullable::create(nested, column_nullable->getNullMapColumnPtr());
        return nullptr;
    }

    if (const auto * column_array = typeid_cast<const ColumnArray *>(&column))
    {
        if (ColumnPtr data = canonicalizeNegativeZero(column_array->getData()))
            return ColumnArray::create(data, column_array->getOffsetsPtr());
        return nullptr;
    }

    if (const auto * column_map = typeid_cast<const ColumnMap *>(&column))
    {
        if (ColumnPtr nested = canonicalizeNegativeZero(column_map->getNestedColumn()))
            return ColumnMap::create(nested);
        return nullptr;
    }

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        auto elements = column_tuple->getColumns();
        bool canonicalized = false;

        for (auto & element : elements)
        {
            if (ColumnPtr canonical_element = canonicalizeNegativeZero(*element))
            {
                element = std::move(canonical_element);
                canonicalized = true;
            }
        }

        if (canonicalized)
            return ColumnTuple::create(elements);
        return nullptr;
    }

    return nullptr;
}

void canonicalizeNegativeZeroInKeyColumns(ColumnRawPtrs & key_columns, Columns & holder)
{
    for (auto & key_column : key_columns)
    {
        if (ColumnPtr canonical = canonicalizeNegativeZero(*key_column))
        {
            holder.emplace_back(std::move(canonical));
            key_column = holder.back().get();
        }
    }
}

}
