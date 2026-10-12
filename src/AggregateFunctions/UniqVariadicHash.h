#pragma once

#include <city.h>
#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/canonicalizeNegativeZero.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <base/normalizeNegativeZero.h>


namespace DB
{
struct Settings;

/** Hashes a set of arguments to the aggregate function
  *  to calculate the number of unique values
  *  and adds them to the set.
  *
  * Four options (2 x 2)
  *
  * - for approximate calculation, uses a non-cryptographic 64-bit hash function;
  * - for an accurate calculation, uses a cryptographic 128-bit hash function;
  *
  * - for several arguments passed in the usual way;
  * - for one argument-tuple.
  */

template <bool exact, bool for_tuple>
struct UniqVariadicHash;


/// If some arguments are not contiguous, we cannot use simple hash function,
///  because it requires method IColumn::getDataAt to work.
/// Note that we treat single tuple argument in the same way as multiple arguments.
bool isAllArgumentsContiguousInMemory(const DataTypes & argument_types);


/// The values are hashed here by their raw bytes, but negative zero has to hash like
/// positive zero, so that `uniq*` agree with the `equals` function - see `base/normalizeNegativeZero.h`.
/// Floating point columns are intercepted and their values are normalized before hashing.
/// Only top-level floating point arguments (including the elements of a single tuple argument)
/// are handled: a floating point value nested deeper, e.g. inside an `Array` argument,
/// is still hashed by its raw representation.
template <typename F>
bool ALWAYS_INLINE withNormalizedFloatValue(const IColumn & column, size_t row_num, F && f)
{
    if (const auto * column_float64 = typeid_cast<const ColumnFloat64 *>(&column))
    {
        f(normalizeNegativeZero(column_float64->getData()[row_num]));
        return true;
    }
    if (const auto * column_float32 = typeid_cast<const ColumnFloat32 *>(&column))
    {
        f(normalizeNegativeZero(column_float32->getData()[row_num]));
        return true;
    }
    if (const auto * column_bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
    {
        f(normalizeNegativeZero(column_bfloat16->getData()[row_num]));
        return true;
    }
    return false;
}

inline UInt64 ALWAYS_INLINE cityHashValueAt(const IColumn & column, size_t row_num)
{
    UInt64 res = 0;
    if (withNormalizedFloatValue(column, row_num, [&](auto value) { res = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&value), sizeof(value)); }))
        return res;

    auto value = column.getDataAt(row_num);

    /// The raw bytes of the value can be a sequence of floating point values, e.g. for an
    /// `Array(Float64)` argument, and they cannot be canonicalized in place - see `rawFloatValueWidth`.
    if (size_t float_width = rawFloatValueWidth(column))
    {
        PODArrayWithStackMemory<char, 64> canonical(value.size());
        canonicalizeNegativeZeroInRawValue(value, float_width, canonical.data());
        return CityHash_v1_0_2::CityHash64(canonical.data(), canonical.size());
    }

    return CityHash_v1_0_2::CityHash64(value.data(), value.size());
}

inline void ALWAYS_INLINE updateSipHashWithValueAt(const IColumn & column, size_t row_num, SipHash & hash)
{
    if (withNormalizedFloatValue(column, row_num, [&](auto value) { hash.update(value); }))
        return;

    column.updateHashWithValue(row_num, hash);
}


template <>
struct UniqVariadicHash<false, false>
{
    static UInt64 apply(size_t num_args, const IColumn ** columns, size_t row_num)
    {
        UInt64 hash = 0;

        const IColumn ** column = columns;
        const IColumn ** columns_end = column + num_args;

        {
            hash = cityHashValueAt(**column, row_num);
            ++column;
        }

        while (column < columns_end)
        {
            hash = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(cityHashValueAt(**column, row_num), hash));
            ++column;
        }

        return hash;
    }
};

template <>
struct UniqVariadicHash<false, true>
{
    static UInt64 apply(size_t num_args, const IColumn ** columns, size_t row_num)
    {
        if (!num_args)
            return 0;

        UInt64 hash = 0;

        const auto & tuple_columns = assert_cast<const ColumnTuple *>(columns[0])->getColumns();

        const auto * column = tuple_columns.data();
        const auto * columns_end = column + num_args;

        {
            hash = cityHashValueAt(**column, row_num);
            ++column;
        }

        while (column < columns_end)
        {
            hash = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(cityHashValueAt(**column, row_num), hash));
            ++column;
        }

        return hash;
    }
};

template <>
struct UniqVariadicHash<true, false>
{
    static UInt128 apply(size_t num_args, const IColumn ** columns, size_t row_num)
    {
        const IColumn ** column = columns;
        const IColumn ** columns_end = column + num_args;

        SipHash hash;

        while (column < columns_end)
        {
            updateSipHashWithValueAt(**column, row_num, hash);
            ++column;
        }

        return hash.get128();
    }
};

template <>
struct UniqVariadicHash<true, true>
{
    static UInt128 apply(size_t num_args, const IColumn ** columns, size_t row_num)
    {
        const auto & tuple_columns = assert_cast<const ColumnTuple *>(columns[0])->getColumns();

        const auto * column = tuple_columns.data();
        const auto * columns_end = column + num_args;

        SipHash hash;

        while (column < columns_end)
        {
            updateSipHashWithValueAt(**column, row_num, hash);
            ++column;
        }

        return hash.get128();
    }
};

}
