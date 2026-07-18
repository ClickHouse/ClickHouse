#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
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

    Field converted = convertFieldToType(field, to, &from, format_settings, strict, convert_inexact_floats);
    if (converted.isNull())
        return {};

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
    catch (...) /// NOLINT(bugprone-empty-catch)
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
    ColumnPtr result = convertColumnToTypeOrNull(value, from, to, format_settings, /*strict=*/false, convert_inexact_floats);
    if (!result)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Value in column of type {} cannot be safely converted into type {}",
            from.getName(),
            to.getName());
    return result;
}

}
