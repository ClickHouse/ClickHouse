#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
}

ColumnPtr convertColumnToTypeOrNull(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Delegation path (step 1): materialize a `Field`, reuse `convertFieldToType`, rebuild a column.
    /// Column-native fast paths (numeric, ...) that avoid the `Field` will replace this incrementally;
    /// the differential test pins the behavior so those replacements cannot change results.
    Field field;
    value.get(0, field);

    const Field converted = convertFieldToType(field, to, &from, format_settings, strict, convert_inexact_floats);

    if (converted.isNull())
    {
        /// `convertFieldToType` returns a Null `Field` for two different outcomes: a legitimate NULL
        /// result (NULL input into a type that can hold NULL) and "not representable". Distinguish
        /// them here instead of collapsing both into a null `ColumnPtr`: a valid NULL becomes a
        /// size-1 column holding NULL, while "not representable" is the null `ColumnPtr{}`.
        if (field.isNull() && canContainNull(to))
        {
            auto null_column = to.createColumn();
            null_column->insert(Field());
            return null_column;
        }
        return {};
    }

    auto column = to.createColumn();
    column->insert(converted);
    return column;
}

ColumnPtr tryConvertColumnToTypeOrNull(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    try
    {
        return convertColumnToTypeOrNull(value, from, to, format_settings, strict, convert_inexact_floats);
    }
    catch (...) // Ok: try-pattern that intentionally maps any conversion failure to a null result
    {
        return {};
    }
}

ColumnPtr convertColumnToTypeOrThrow(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Mirror `convertFieldToTypeOrThrow`: a NULL that the target cannot hold is a type mismatch,
    /// while a non-NULL value that does not fit the target is out of range.
    if (value.isNullAt(0) && !canContainNull(to))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to.getName());

    ColumnPtr result = convertColumnToTypeOrNull(value, from, to, format_settings, /*strict=*/false, convert_inexact_floats);

    if (!value.isNullAt(0) && !result)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Value in column of type {} cannot be represented as {}",
            from.getName(),
            to.getName());

    return result;
}

}
