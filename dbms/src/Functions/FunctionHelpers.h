#pragma once

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/callOnTypeIndex.h>


namespace DB
{

class IFunction;

/// Methods, that helps dispatching over real column types.

template <typename Type>
const Type * checkAndGetDataType(const IDataType * data_type)
{
    return typeid_cast<const Type *>(data_type);
}

template <typename Type>
const ColumnConst * checkAndGetColumnConst(const IColumn * column)
{
    if (!column || !isColumnConst(*column))
        return {};

    const ColumnConst * res = assert_cast<const ColumnConst *>(column);

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
inline std::enable_if_t<!IsDecimalNumber<T>, Field> toField(const T & x)
{
    return Field(NearestFieldType<T>(x));
}

template <typename T>
inline std::enable_if_t<IsDecimalNumber<T>, Field> toField(const T & x, UInt32 scale)
{
    return Field(NearestFieldType<T>(x, scale));
}


Columns convertConstTupleToConstantElements(const ColumnConst & column);


/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args);

/// Similar function as above. Additionally transform the result type if needed.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args, size_t result);

/// Checks argument type at specified index with predicate.
/// throws if there is no argument at specified index or if predicate returns false.
void validateArgumentType(const IFunction & func, const DataTypes & arguments,
        size_t argument_index, bool (* validator_func)(const IDataType &),
        const char * expected_type_description);

/// Checks if a list of array columns have equal offsets. Return a pair of nested columns and offsets if true, otherwise throw.
std::pair<std::vector<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments);

}
