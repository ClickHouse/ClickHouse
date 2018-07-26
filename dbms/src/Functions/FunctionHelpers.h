#pragma once

#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>


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


/// Unroll template using TypeNumber<T>
template <template <typename, typename, typename> typename Apply, typename Op, typename... Args>
inline bool callByNumber(size_t type_num1, size_t type_num2, Args &... args)
{
    static_assert(TypeNumber<UInt8>::value < 16);
    static_assert(TypeNumber<UInt16>::value < 16);
    static_assert(TypeNumber<UInt32>::value < 16);
    static_assert(TypeNumber<UInt64>::value < 16);
    static_assert(TypeNumber<UInt128>::value < 16);

    static_assert(TypeNumber<Int8>::value < 16);
    static_assert(TypeNumber<Int16>::value < 16);
    static_assert(TypeNumber<Int32>::value < 16);
    static_assert(TypeNumber<Int64>::value < 16);
    static_assert(TypeNumber<Int128>::value < 16);

    if (type_num1 >= 16 || type_num2 >= 16)
        return false;

    UInt8 number = (type_num1 << 4) + type_num2;
    switch (number)
    {
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<UInt8>::value:        Apply<UInt8, UInt8, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<UInt16>::value:       Apply<UInt8, UInt16, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<UInt32>::value:       Apply<UInt8, UInt32, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<UInt64>::value:       Apply<UInt8, UInt64, Op>(args...); break;
        //case (TypeNumber<UInt8>::value << 4) + TypeNumber<UInt128>::value:      Apply<UInt8, UInt128, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<Int8>::value:         Apply<UInt8, Int8, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<Int16>::value:        Apply<UInt8, Int16, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<Int32>::value:        Apply<UInt8, Int32, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<Int64>::value:        Apply<UInt8, Int64, Op>(args...); break;
        case (TypeNumber<UInt8>::value << 4) + TypeNumber<Int128>::value:       Apply<UInt8, Int128, Op>(args...); break;

        case (TypeNumber<UInt16>::value << 4) + TypeNumber<UInt8>::value:       Apply<UInt16, UInt8, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<UInt16>::value:      Apply<UInt16, UInt16, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<UInt32>::value:      Apply<UInt16, UInt32, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<UInt64>::value:      Apply<UInt16, UInt64, Op>(args...); break;
        //case (TypeNumber<UInt16>::value << 4) + TypeNumber<UInt128>::value:      Apply<UInt16, UInt128, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<Int8>::value:        Apply<UInt16, Int8, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<Int16>::value:       Apply<UInt16, Int16, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<Int32>::value:       Apply<UInt16, Int32, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<Int64>::value:       Apply<UInt16, Int64, Op>(args...); break;
        case (TypeNumber<UInt16>::value << 4) + TypeNumber<Int128>::value:      Apply<UInt16, Int128, Op>(args...); break;

        case (TypeNumber<UInt32>::value << 4) + TypeNumber<UInt8>::value:       Apply<UInt32, UInt8, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<UInt16>::value:      Apply<UInt32, UInt16, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<UInt32>::value:      Apply<UInt32, UInt32, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<UInt64>::value:      Apply<UInt32, UInt64, Op>(args...); break;
        //case (TypeNumber<UInt32>::value << 4) + TypeNumber<UInt128>::value:      Apply<UInt32, UInt128, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<Int8>::value:        Apply<UInt32, Int8, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<Int16>::value:       Apply<UInt32, Int16, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<Int32>::value:       Apply<UInt32, Int32, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<Int64>::value:       Apply<UInt32, Int64, Op>(args...); break;
        case (TypeNumber<UInt32>::value << 4) + TypeNumber<Int128>::value:      Apply<UInt32, Int128, Op>(args...); break;

        case (TypeNumber<UInt64>::value << 4) + TypeNumber<UInt8>::value:       Apply<UInt64, UInt8, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<UInt16>::value:      Apply<UInt64, UInt16, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<UInt32>::value:      Apply<UInt64, UInt32, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<UInt64>::value:      Apply<UInt64, UInt64, Op>(args...); break;
        //case (TypeNumber<UInt64>::value << 4) + TypeNumber<UInt128>::value:     Apply<UInt64, UInt128, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<Int8>::value:        Apply<UInt64, Int8, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<Int16>::value:       Apply<UInt64, Int16, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<Int32>::value:       Apply<UInt64, Int32, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<Int64>::value:       Apply<UInt64, Int64, Op>(args...); break;
        case (TypeNumber<UInt64>::value << 4) + TypeNumber<Int128>::value:      Apply<UInt64, Int128, Op>(args...); break;
#if 0
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<UInt8>::value:      Apply<UInt128, UInt8, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<UInt16>::value:     Apply<UInt128, UInt16, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<UInt32>::value:     Apply<UInt128, UInt32, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<UInt64>::value:     Apply<UInt128, UInt64, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<UInt128>::value:    Apply<UInt128, UInt128, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<Int8>::value:       Apply<UInt128, Int8, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<Int16>::value:      Apply<UInt128, Int16, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<Int32>::value:      Apply<UInt128, Int32, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<Int64>::value:      Apply<UInt128, Int64, Op>(args...); break;
        case (TypeNumber<UInt128>::value << 4) + TypeNumber<Int128>::value:     Apply<UInt128, Int128, Op>(args...); break;
#endif
        case (TypeNumber<Int8>::value << 4) + TypeNumber<UInt8>::value:         Apply<Int8, UInt8, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<UInt16>::value:        Apply<Int8, UInt16, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<UInt32>::value:        Apply<Int8, UInt32, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<UInt64>::value:        Apply<Int8, UInt64, Op>(args...); break;
        //case (TypeNumber<Int8>::value << 4) + TypeNumber<UInt128>::value:       Apply<Int8, UInt128, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<Int8>::value:          Apply<Int8, Int8, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<Int16>::value:         Apply<Int8, Int16, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<Int32>::value:         Apply<Int8, Int32, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<Int64>::value:         Apply<Int8, Int64, Op>(args...); break;
        case (TypeNumber<Int8>::value << 4) + TypeNumber<Int128>::value:        Apply<Int8, Int128, Op>(args...); break;

        case (TypeNumber<Int16>::value << 4) + TypeNumber<UInt8>::value:        Apply<Int16, UInt8, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<UInt16>::value:       Apply<Int16, UInt16, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<UInt32>::value:       Apply<Int16, UInt32, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<UInt64>::value:       Apply<Int16, UInt64, Op>(args...); break;
        //case (TypeNumber<Int16>::value << 4) + TypeNumber<UInt128>::value:      Apply<Int16, UInt128, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<Int8>::value:         Apply<Int16, Int8, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<Int16>::value:        Apply<Int16, Int16, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<Int32>::value:        Apply<Int16, Int32, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<Int64>::value:        Apply<Int16, Int64, Op>(args...); break;
        case (TypeNumber<Int16>::value << 4) + TypeNumber<Int128>::value:       Apply<Int16, Int128, Op>(args...); break;

        case (TypeNumber<Int32>::value << 4) + TypeNumber<UInt8>::value:        Apply<Int32, UInt8, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<UInt16>::value:       Apply<Int32, UInt16, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<UInt32>::value:       Apply<Int32, UInt32, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<UInt64>::value:       Apply<Int32, UInt64, Op>(args...); break;
        //case (TypeNumber<Int32>::value << 4) + TypeNumber<UInt128>::value:      Apply<Int32, UInt128, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<Int8>::value:         Apply<Int32, Int8, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<Int16>::value:        Apply<Int32, Int16, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<Int32>::value:        Apply<Int32, Int32, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<Int64>::value:        Apply<Int32, Int64, Op>(args...); break;
        case (TypeNumber<Int32>::value << 4) + TypeNumber<Int128>::value:       Apply<Int32, Int128, Op>(args...); break;

        case (TypeNumber<Int64>::value << 4) + TypeNumber<UInt8>::value:        Apply<Int64, UInt8, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<UInt16>::value:       Apply<Int64, UInt16, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<UInt32>::value:       Apply<Int64, UInt32, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<UInt64>::value:       Apply<Int64, UInt64, Op>(args...); break;
        //case (TypeNumber<Int64>::value << 4) + TypeNumber<UInt128>::value:      Apply<Int64, UInt128, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<Int8>::value:         Apply<Int64, Int8, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<Int16>::value:        Apply<Int64, Int16, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<Int32>::value:        Apply<Int64, Int32, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<Int64>::value:        Apply<Int64, Int64, Op>(args...); break;
        case (TypeNumber<Int64>::value << 4) + TypeNumber<Int128>::value:       Apply<Int64, Int128, Op>(args...); break;

        case (TypeNumber<Int128>::value << 4) + TypeNumber<UInt8>::value:       Apply<Int128, UInt8, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<UInt16>::value:      Apply<Int128, UInt16, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<UInt32>::value:      Apply<Int128, UInt32, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<UInt64>::value:      Apply<Int128, UInt64, Op>(args...); break;
        //case (TypeNumber<Int128>::value << 4) + TypeNumber<UInt128>::value:     Apply<Int128, UInt128, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<Int8>::value:        Apply<Int128, Int8, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<Int16>::value:       Apply<Int128, Int16, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<Int32>::value:       Apply<Int128, Int32, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<Int64>::value:       Apply<Int128, Int64, Op>(args...); break;
        case (TypeNumber<Int128>::value << 4) + TypeNumber<Int128>::value:      Apply<Int128, Int128, Op>(args...); break;

        default:
            return false;
    };

    return true;
}

}
