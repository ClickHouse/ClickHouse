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
    if (!column->isConst())
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
        return res;

    return &res->getDataColumn();
}

template <typename Type>
const bool checkColumnConst(const IColumn * column)
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


ColumnPtr convertConstTupleToTupleOfConstants(const ColumnConst & column);


/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args);

/// Similar function as above. Additionally transform the result type if needed.
Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result);


template <typename F>
bool dispatchForFirstType(F && f)
{
    return false;
}

template <typename HeadArg, typename... TailArgs, typename F>
bool dispatchForFirstType(F && f)
{
    if (f(static_cast<HeadArg *>(nullptr)))
        return true;
    return dispatchForFirstType<TailArgs...>(std::forward<F>(f));
}


template <typename F>
bool dispatchForFirstNumericType(F && f)
{
    return dispatchForFirstType<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>(std::forward<F>(f));
}

template <typename F>
bool dispatchForFirstIntegerType(F && f)
{
    return dispatchForFirstType<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64>(std::forward<F>(f));
}

}
