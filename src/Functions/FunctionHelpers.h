#pragma once

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/demangle.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Core/ColumnsWithTypeAndName.h>

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
    [[nodiscard]] int isValid(const DataTypePtr & data_type, const ColumnPtr & column) const;
};

using FunctionArgumentDescriptors = VectorWithMemoryTracking<FunctionArgumentDescriptor>;

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

void validateFunctionArguments(const String & function_name, const ColumnsWithTypeAndName & arguments,
                               const FunctionArgumentDescriptors & mandatory_args,
                               const FunctionArgumentDescriptors & optional_args = {});

/// Validates that the user-provided arguments match the expected arguments and the variadic argument at the end of the argument list.
///
/// Checks that
/// - the number of provided arguments is at least the number of mandatory arguments,
/// - all mandatory arguments are present and have the right type,
/// - all arguments provided in excess of the mandatory arguments match the variadic argument type.
///
/// With multiple variadic arguments, e.g. f(a, b, ...[c]), provided arguments must match left-to-right. E.g. these calls are considered valid:
///     f(a, b)
///     f(a, b, c)
///     f(a, b, c, c)
/// but these are NOT:
///     f(a)
///     f(a, b, b)
///     f(a, c)
void validateFunctionArgumentsWithVariadics(const IFunction & func, const ColumnsWithTypeAndName & arguments,
                                            const FunctionArgumentDescriptors & mandatory_args,
                                            const FunctionArgumentDescriptor & variadic_args);

/// Validates that the number of provided arguments is within expected range.
void validateNumberOfFunctionArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments,
                                       size_t expected_min_args,
                                       size_t expected_max_args);

/// Checks if a list of array columns have equal offsets. Return a pair of nested columns and offsets if true, otherwise throw.
std::pair<VectorWithMemoryTracking<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments);

/// Return ColumnNullable of src, with null map as OR-ed null maps of args columns.
/// Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
ColumnPtr
wrapInNullable(const ColumnPtr & src, const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count);

/** Return ColumnNullable of src, with input null map
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  * If null_map is nullptr, it's as if it's all 0.
  */
ColumnPtr wrapInNullable(const ColumnPtr & src, ColumnPtr null_map);

struct NullPresence
{
    bool has_nullable = false; /// Nullable(T)
    bool has_null_constant = false; /// Nullable(Nothing)
};

NullPresence getNullPresense(const ColumnsWithTypeAndName & args);

struct FunctionResultSetting;

/// For `resultDependsOnSetting` of a function that converts a value of type `from` to `to` (a `Dynamic`, `Variant` or `JSON` value
/// counts as any type, since it holds one at runtime):
/// - the settings of parsing text apply only when `from` has text, of writing text only when `to` has text, and the spellings of
///   `Bool` when either has text;
/// - the settings of `JSON` apply only when either has `JSON`, and of type inference only when `to` has a `Dynamic`, `Variant` or
///   `JSON`;
/// - `session_timezone` applies when a `DateTime` or `DateTime64` without an explicit time zone is written as text, when a type is
///   inferred from text, when `to` has such a time that the function creates (`creates_result_type`), which takes the time zone of
///   the session, and otherwise when text or a value without a time zone (`Date`, `Time`) becomes such a time.
/// Any other setting always applies.
bool conversionResultDependsOnSetting(
    const FunctionResultSetting & setting, const DataTypePtr & from, const DataTypePtr & to, bool creates_result_type = true);

/// For `resultDependsOnSetting` of `if`, `multiIf` and `transform`: `optimize_if_transform_const_strings_to_lowcardinality` and
/// `optimize_if_transform_strings_to_enum` change only a result of text, any other setting always applies.
bool stringBranchesResultDependsOnSetting(const FunctionResultSetting & setting, const DataTypePtr & result_type);

bool isDecimalOrNullableDecimal(const DataTypePtr & type);
bool isLowCardinalityType(const IDataType & type);
/// Returns true if any of the argument types is or contains LowCardinality
/// (e.g. LowCardinality(UInt8), Array(LowCardinality(String)) or Map(LowCardinality(String), String)).
bool hasLowCardinalityTypes(const ColumnsWithTypeAndName & args);
/// Returns true if all of the arguments have constant columns.
bool allArgumentColumnsAreConstant(const ColumnsWithTypeAndName & args);
/// Whether `plus`/`minus` is injective in its varying argument, given the other one fixed. Only
/// integer arithmetic qualifies: integer wrap-around is a bijection, while every other operand class
/// collapses distinct arguments somewhere - an `Interval` at end-of-month days and DST transitions, a
/// float or `Decimal` by rounding or rescaling, a narrower date constant, a NULL constant.
bool plusMinusWithConstantsIsInjective(
    const ColumnWithTypeAndName & left, const ColumnWithTypeAndName & right, const DataTypePtr & return_type);
bool convertLowCardinalityColumnsToFull(ColumnsWithTypeAndName & args);

void checkFunctionArgumentSizes(const ColumnsWithTypeAndName & arguments, size_t input_rows_count);

/// Casts a `Nullable(Enum)` column, or a `Nullable` column of a `Tuple`/`Object` containing an `Enum`, to the
/// `Nullable` form of `result_type`. A cast of its nested column instead would also convert the values under
/// `NULL`, which can be invalid, e.g. `0` for an `Enum` without such an element.
ColumnWithTypeAndName castNullableEnumToResultType(const ColumnWithTypeAndName & argument, const DataTypePtr & result_type);

/// Throws VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE if a UInt64 argument holds 2^63 or more, which `IColumn::getInt`
/// would read as negative. Accepts a constant or an ordinary column.
void checkIntegerArgumentFitsInt64(const IColumn & column, std::string_view function_name, std::string_view argument_name);

/// Whether a UInt64 argument may hold values that are not the input of any row with a computed result, which
/// `checkIntegerArgumentFitsInt64` must not reject: the value of a row whose result is NULL, because some argument
/// (the same or another one) is Nullable, or a dictionary entry of a `LowCardinality` argument that no row references.
/// A function returns the negation from `canBeExecutedOnDefaultArguments`, so that the framework drops such values.
bool uint64ArgumentMayHoldUnusedValues(const ColumnsWithTypeAndName & arguments);
}
