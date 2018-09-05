#pragma once

#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>

namespace common
{
    template <typename T>
    inline bool addOverflow(T x, T y, T & res)
    {
        return __builtin_add_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(Int32 x, Int32 y, Int32 & res)
    {
        return __builtin_sadd_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long x, long y, long & res)
    {
        return __builtin_saddl_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(long long x, long long y, long long & res)
    {
        return __builtin_saddll_overflow(x, y, &res);
    }

    template <>
    inline bool addOverflow(__int128 x, __int128 y, __int128 & res)
    {
        res = x + y;
        return (res - y) != x;
    }

    template <typename T>
    inline bool subOverflow(T x, T y, T & res)
    {
        return __builtin_sub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(Int32 x, Int32 y, Int32 & res)
    {
        return __builtin_ssub_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long x, long y, long & res)
    {
        return __builtin_ssubl_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(long long x, long long y, long long & res)
    {
        return __builtin_ssubll_overflow(x, y, &res);
    }

    template <>
    inline bool subOverflow(__int128 x, __int128 y, __int128 & res)
    {
        res = x - y;
        return (res + y) != x;
    }

    template <typename T>
    inline bool mulOverflow(T x, T y, T & res)
    {
        return __builtin_mul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(Int32 x, Int32 y, Int32 & res)
    {
        return __builtin_smul_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long x, long y, long & res)
    {
        return __builtin_smull_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(long long x, long long y, long long & res)
    {
        return __builtin_smulll_overflow(x, y, &res);
    }

    template <>
    inline bool mulOverflow(__int128 x, __int128 y, __int128 & res)
    {
        res = x * y;
        return (res / y) != x;
    }
}

namespace DB
{

/// Methods, that helps dispatching over real column types.

template <typename Type>
const Type * checkAndGetDataType(const IDataType * data_type)
{
    return typeid_cast<const Type *>(data_type);
}

template <typename Type>
bool checkDataType(const IDataType * data_type)
{
    return checkAndGetDataType<Type>(data_type);
}


template <typename Type>
const Type * checkAndGetColumn(const IColumn * column)
{
    return typeid_cast<const Type *>(column);
}

template <typename Type>
bool checkColumn(const IColumn * column)
{
    return checkAndGetColumn<Type>(column);
}


template <typename Type>
const ColumnConst * checkAndGetColumnConst(const IColumn * column)
{
    if (!column || !column->isColumnConst())
        return {};

    const ColumnConst * res = static_cast<const ColumnConst *>(column);

    if (!checkColumn<Type>(&res->getDataColumn()))
        return {};

    return res;
}

template <typename Type>
const Type * checkAndGetColumnConstData(const IColumn * column)
{
    const ColumnConst * res = checkAndGetColumnConst<Type>(column);

    if (!res)
        return {};

    return static_cast<const Type *>(&res->getDataColumn());
}

template <typename Type>
bool checkColumnConst(const IColumn * column)
{
    return checkAndGetColumnConst<Type>(column);
}


/// Returns non-nullptr if column is ColumnConst with ColumnString or ColumnFixedString inside.
const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column);


/// Transform anything to Field.
template <typename T>
inline Field toField(const T & x)
{
    return Field(typename NearestFieldType<T>::Type(x));
}


Columns convertConstTupleToConstantElements(const ColumnConst & column);


/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args);

/// Similar function as above. Additionally transform the result type if needed.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args, size_t result);

template <typename T, typename F>
bool callByTypeAndNumber(UInt8 number, F && f)
{
    switch (number)
    {
        case TypeId<UInt8>::value:        f(T(), UInt8()); break;
        case TypeId<UInt16>::value:       f(T(), UInt16()); break;
        case TypeId<UInt32>::value:       f(T(), UInt32()); break;
        case TypeId<UInt64>::value:       f(T(), UInt64()); break;
        //case TypeId<UInt128>::value:      f(T(), UInt128()); break;

        case TypeId<Int8>::value:         f(T(), Int8()); break;
        case TypeId<Int16>::value:        f(T(), Int16()); break;
        case TypeId<Int32>::value:        f(T(), Int32()); break;
        case TypeId<Int64>::value:        f(T(), Int64()); break;
        case TypeId<Int128>::value:       f(T(), Int128()); break;

        case TypeId<Decimal32>::value:        f(T(), Decimal32()); break;
        case TypeId<Decimal64>::value:        f(T(), Decimal64()); break;
        case TypeId<Decimal128>::value:       f(T(), Decimal128()); break;
        default:
            return false;
    }

    return true;
}

/// Unroll template using TypeNumber<T>
template <typename F>
inline bool callByNumbers(UInt8 type_num1, UInt8 type_num2, F && f)
{
    switch (type_num1)
    {
        case TypeId<UInt8>::value: return callByTypeAndNumber<UInt8>(type_num2, std::forward<F>(f));
        case TypeId<UInt16>::value: return callByTypeAndNumber<UInt16>(type_num2, std::forward<F>(f));
        case TypeId<UInt32>::value: return callByTypeAndNumber<UInt32>(type_num2, std::forward<F>(f));
        case TypeId<UInt64>::value: return callByTypeAndNumber<UInt64>(type_num2, std::forward<F>(f));
        //case TypeId<UInt128>::value: return callByTypeAndNumber<UInt128>(type_num2, std::forward<F>(f));

        case TypeId<Int8>::value: return callByTypeAndNumber<Int8>(type_num2, std::forward<F>(f));
        case TypeId<Int16>::value: return callByTypeAndNumber<Int16>(type_num2, std::forward<F>(f));
        case TypeId<Int32>::value: return callByTypeAndNumber<Int32>(type_num2, std::forward<F>(f));
        case TypeId<Int64>::value: return callByTypeAndNumber<Int64>(type_num2, std::forward<F>(f));
        case TypeId<Int128>::value: return callByTypeAndNumber<Int128>(type_num2, std::forward<F>(f));

        case TypeId<Decimal32>::value: return callByTypeAndNumber<Decimal32>(type_num2, std::forward<F>(f));
        case TypeId<Decimal64>::value: return callByTypeAndNumber<Decimal64>(type_num2, std::forward<F>(f));
        case TypeId<Decimal128>::value: return callByTypeAndNumber<Decimal128>(type_num2, std::forward<F>(f));

        default:
            break;
    };

    return false;
}

}
