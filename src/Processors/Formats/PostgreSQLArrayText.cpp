#include <Processors/Formats/PostgreSQLArrayText.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{

/// Emit one scalar array element: double-quoted, with `"` and `\` escaped.
void writeQuotedElement(const String & value, WriteBuffer & out)
{
    writeChar('"', out);
    for (const char c : value)
    {
        if (c == '"' || c == '\\')
            writeChar('\\', out);
        writeChar(c, out);
    }
    writeChar('"', out);
}

}

void writePostgreSQLArrayText(
    const IColumn & column, const IDataType & type, size_t row, WriteBuffer & out, const FormatSettings & settings)
{
    /// A constant expression (e.g. `SELECT [1, 2]`) may reach here as a `ColumnConst` if the caller
    /// does not materialize its input; unwrap it instead of copying the whole column per row.
    if (const auto * const_column = checkAndGetColumn<ColumnConst>(&column))
    {
        writePostgreSQLArrayText(const_column->getDataColumn(), type, 0, out, settings);
        return;
    }

    const auto & array_column = assert_cast<const ColumnArray &>(column);
    const auto & nested_type = assert_cast<const DataTypeArray &>(type).getNestedType();
    const IColumn & nested_column = array_column.getData();
    const auto & offsets = array_column.getOffsets();

    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t end = offsets[row];
    const bool nested_is_array = isArray(nested_type);
    const auto nested_serialization = nested_type->getDefaultSerialization();

    writeChar('{', out);
    for (size_t k = begin; k < end; ++k)
    {
        if (k != begin)
            writeChar(',', out);

        if (nested_is_array)
        {
            writePostgreSQLArrayText(nested_column, *nested_type, k, out, settings);
        }
        else if (nested_column.isNullAt(k))
        {
            writeCString("NULL", out);
        }
        else
        {
            WriteBufferFromOwnString element;
            nested_serialization->serializeText(nested_column, k, element, settings);
            writeQuotedElement(element.str(), out);
        }
    }
    writeChar('}', out);
}

}
