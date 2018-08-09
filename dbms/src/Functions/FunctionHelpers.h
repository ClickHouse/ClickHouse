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

template <typename T, template <typename, typename, template <typename, typename> typename> typename Apply,
          template <typename, typename> typename Op, typename... Args>
void callByTypeAndNumber(UInt8 number, bool & done, Args &... args)
{
    done = true;
    switch (number)
    {
        case TypeNumber<UInt8>::value:        Apply<T, UInt8, Op>(args...); break;
        case TypeNumber<UInt16>::value:       Apply<T, UInt16, Op>(args...); break;
        case TypeNumber<UInt32>::value:       Apply<T, UInt32, Op>(args...); break;
        case TypeNumber<UInt64>::value:       Apply<T, UInt64, Op>(args...); break;
        //case TypeNumber<UInt128>::value:      Apply<T, UInt128, Op>(args...); break;

        case TypeNumber<Int8>::value:         Apply<T, Int8, Op>(args...); break;
        case TypeNumber<Int16>::value:        Apply<T, Int16, Op>(args...); break;
        case TypeNumber<Int32>::value:        Apply<T, Int32, Op>(args...); break;
        case TypeNumber<Int64>::value:        Apply<T, Int64, Op>(args...); break;
        case TypeNumber<Int128>::value:       Apply<T, Int128, Op>(args...); break;

        case TypeNumber<Dec32>::value:        Apply<T, Dec32, Op>(args...); break;
        case TypeNumber<Dec64>::value:        Apply<T, Dec64, Op>(args...); break;
        case TypeNumber<Dec128>::value:       Apply<T, Dec128, Op>(args...); break;
        default:
            done = false;
    }
}

/// Unroll template using TypeNumber<T>
template <template <typename, typename, template <typename, typename> typename> typename Apply,
          template <typename, typename> typename Op, typename... Args>
inline bool callByNumbers(UInt8 type_num1, UInt8 type_num2, Args &... args)
{
    bool done = false;
    switch (type_num1)
    {
        case TypeNumber<UInt8>::value: callByTypeAndNumber<UInt8, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<UInt16>::value: callByTypeAndNumber<UInt16, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<UInt32>::value: callByTypeAndNumber<UInt32, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<UInt64>::value: callByTypeAndNumber<UInt64, Apply, Op, Args...>(type_num2, done, args...); break;
        //case TypeNumber<UInt128>::value: callByTypeAndNumber<UInt128, Apply, Op, Args...>(type_num2, done, args...); break;

        case TypeNumber<Int8>::value: callByTypeAndNumber<Int8, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Int16>::value: callByTypeAndNumber<Int16, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Int32>::value: callByTypeAndNumber<Int32, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Int64>::value: callByTypeAndNumber<Int64, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Int128>::value: callByTypeAndNumber<Int128, Apply, Op, Args...>(type_num2, done, args...); break;

        case TypeNumber<Dec32>::value: callByTypeAndNumber<Dec32, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Dec64>::value: callByTypeAndNumber<Dec64, Apply, Op, Args...>(type_num2, done, args...); break;
        case TypeNumber<Dec128>::value: callByTypeAndNumber<Dec128, Apply, Op, Args...>(type_num2, done, args...); break;

        default:
            break;
    };

    return done;
}

}
