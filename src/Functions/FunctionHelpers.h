#pragma once

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/callOnTypeIndex.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class IFunction;

/// Methods, that helps dispatching over real column types.

template <typename Type>
const Type * checkAndGetDataType(const IDataType * data_type)
{
    return typeid_cast<const Type *>(data_type);
}

/// Throws on mismatch.
template <typename Type>
const Type & checkAndGetDataType(const IDataType & data_type)
{
    return typeid_cast<const Type &>(data_type);
}

template <typename... Types>
bool checkDataTypes(const IDataType * data_type)
{
    return (... || typeid_cast<const Types *>(data_type));
}

template <typename Type>
const ColumnConst * checkAndGetColumnConst(const IColumn * column)
{
    if (!column)
        return {};

    const ColumnConst * res = checkAndGetColumn<ColumnConst>(column);
    if (!res)
        return {};

    if (!checkColumn<Type>(&res->getDataColumn()))
        return {};

    return res;
}

template <typename Type>
const ColumnConst & checkAndGetColumnConst(const IColumn & column)
{
    const ColumnConst & res = checkAndGetColumn<ColumnConst>(column);

    const auto & data_column = res.getDataColumn();
    if (!checkColumn<Type>(&data_column))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unexpected const column type: expected {}, got {}", demangle(typeid(Type).name()), demangle(typeid(data_column).name()));

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
Field toField(const T & x)
{
    return Field(NearestFieldType<T>(x));
}

template <is_decimal T>
Field toField(const T & x, UInt32 scale)
{
    return Field(NearestFieldType<T>(x, scale));
}


Columns convertConstTupleToConstantElements(const ColumnConst & column);

/// Returns nested column with corrected type if nullable
ColumnWithTypeAndName columnGetNested(const ColumnWithTypeAndName & col);

/// Returns the copy of a given columns in which each column is replaced with its respective nested
/// column if it is nullable.
ColumnsWithTypeAndName createBlockWithNestedColumns(const ColumnsWithTypeAndName & columns);

/// Expected arguments for a function. Can be used in conjunction with validateFunctionArguments() to check that the user-provided
/// arguments match the expected arguments.
struct FunctionArgumentDescriptor
{
    /// The argument name, e.g. "longitude".
    /// Should not be empty.
    std::string_view name;

    /// A function which validates the argument data type.
    /// May be nullptr.
    using TypeValidator = bool (*)(const IDataType &);
    TypeValidator type_validator;

    /// A function which validates the argument column.
    /// May be nullptr.
    using ColumnValidator = bool (*)(const IColumn &);
    ColumnValidator column_validator;

    /// The expected argument type, e.g. "const String" or "UInt64".
    /// Should not be empty.
    std::string_view type_name;

    /// Validate argument type and column.
    int isValid(const DataTypePtr & data_type, const ColumnPtr & column) const;
};

using FunctionArgumentDescriptors = std::vector<FunctionArgumentDescriptor>;

/// Validates that the user-provided arguments match the expected arguments.
///
/// Checks that
/// - the number of provided arguments matches the number of mandatory/optional arguments,
/// - all mandatory arguments are present and have the right type,
/// - optional arguments - if present - have the right type.
///
/// With multiple optional arguments, e.g. f([a, b, c]), provided arguments must match left-to-right. E.g. these calls are considered valid:
///     f(a)
///     f(a, b)
///     f(a, b, c)
/// but these are NOT:
///     f(a, c)
///     f(b, c)
void validateFunctionArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments,
                               const FunctionArgumentDescriptors & mandatory_args,
                               const FunctionArgumentDescriptors & optional_args = {});

/// Checks if a list of array columns have equal offsets. Return a pair of nested columns and offsets if true, otherwise throw.
std::pair<std::vector<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments);

/// Return ColumnNullable of src, with null map as OR-ed null maps of args columns.
/// Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
ColumnPtr wrapInNullable(const ColumnPtr & src, const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count);

struct NullPresence
{
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const ColumnsWithTypeAndName & args);

bool isDecimalOrNullableDecimal(const DataTypePtr & type);

void checkFunctionArgumentSizes(const ColumnsWithTypeAndName & arguments, size_t input_rows_count);
}
