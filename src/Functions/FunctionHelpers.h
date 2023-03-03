#pragma once

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
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

template <typename... Types>
bool checkDataTypes(const IDataType * data_type)
{
    return (... || typeid_cast<const Types *>(data_type));
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

/// Checks argument type at specified index with predicate.
/// throws if there is no argument at specified index or if predicate returns false.
void validateArgumentType(const IFunction & func, const DataTypes & arguments,
        size_t argument_index, bool (* validator_func)(const IDataType &),
        const char * expected_type_description);

/** Simple validator that is used in conjunction with validateFunctionArgumentTypes() to check if function arguments are as expected
 *
 * Also it is used to generate function description when arguments do not match expected ones.
 * Any field can be null:
 *  `argument_name` - if not null, reported via type check errors.
 *  `expected_type_description` - if not null, reported via type check errors.
 *  `type_validator_func` - if not null, used to validate data type of function argument.
 *  `column_validator_func` - if not null, used to validate column of function argument.
 */
struct FunctionArgumentDescriptor
{
    const char * argument_name;

    std::function<bool (const IDataType &)> type_validator_func;
    std::function<bool (const IColumn &)> column_validator_func;

    const char * expected_type_description;

    /** Validate argument type and column.
     *
     * Returns non-zero error code if:
     *     Validator != nullptr && (Value == nullptr || Validator(*Value) == false)
     * For:
     *     Validator is either `type_validator_func` or `column_validator_func`
     *     Value is either `data_type` or `column` respectively.
     * ILLEGAL_TYPE_OF_ARGUMENT if type validation fails
     *
     */
    int isValid(const DataTypePtr & data_type, const ColumnPtr & column) const;
};

using FunctionArgumentDescriptors = std::vector<FunctionArgumentDescriptor>;

/** Validate that function arguments match specification.
 *
 * Designed to simplify argument validation for functions with variable arguments
 * (e.g. depending on result type or other trait).
 * First, checks that number of arguments is as expected (including optional arguments).
 * Second, checks that mandatory args present and have valid type.
 * Third, checks optional arguents types, skipping ones that are missing.
 *
 * Please note that if you have several optional arguments, like f([a, b, c]),
 * only these calls are considered valid:
 *  f(a)
 *  f(a, b)
 *  f(a, b, c)
 *
 * But NOT these: f(a, c), f(b, c)
 * In other words you can't omit middle optional arguments (just like in regular C++).
 *
 * If any mandatory arg is missing, throw an exception, with explicit description of expected arguments.
 */
void validateFunctionArgumentTypes(const IFunction & func, const ColumnsWithTypeAndName & arguments,
                                   const FunctionArgumentDescriptors & mandatory_args,
                                   const FunctionArgumentDescriptors & optional_args = {});

/// Checks if a list of array columns have equal offsets. Return a pair of nested columns and offsets if true, otherwise throw.
std::pair<std::vector<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments);

/// Check if two types are equal
bool areTypesEqual(const IDataType & lhs, const IDataType & rhs);

bool areTypesEqual(const DataTypePtr & lhs, const DataTypePtr & rhs);

/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrapInNullable(const ColumnPtr & src, const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count);

struct NullPresence
{
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const ColumnsWithTypeAndName & args);

bool isDecimalOrNullableDecimal(const DataTypePtr & type);
}
