#include <Processors/Formats/PostgreSQLArrayText.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <Poco/String.h>

#include <optional>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void validatePostgreSQLArrayShape(
    const IColumn & column, const IDataType & type, size_t row, std::vector<std::optional<size_t>> & dimensions, size_t depth)
{
    if (const auto * const_column = checkAndGetColumn<ColumnConst>(&column))
    {
        validatePostgreSQLArrayShape(const_column->getDataColumn(), type, 0, dimensions, depth);
        return;
    }

    const auto & array_column = assert_cast<const ColumnArray &>(column);
    const auto & array_type = assert_cast<const DataTypeArray &>(type);
    const auto & offsets = array_column.getOffsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t end = offsets[row];
    const size_t size = end - begin;

    if (dimensions.size() == depth)
        dimensions.emplace_back(size);
    else if (*dimensions[depth] != size)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot serialize {} as a PostgreSQL array: multidimensional PostgreSQL arrays must be rectangular",
            type.getName());

    const auto & nested_type = array_type.getNestedType();
    if (!isArray(nested_type))
        return;

    const IColumn & nested_column = array_column.getData();
    for (size_t k = begin; k < end; ++k)
        validatePostgreSQLArrayShape(nested_column, *nested_type, k, dimensions, depth + 1);
}

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
    std::vector<std::optional<size_t>> dimensions;
    validatePostgreSQLArrayShape(column, type, row, dimensions, 0);

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

namespace
{

/// Recursive-descent parser for the PostgreSQL array-literal grammar:
///     array   := '{' [ element (',' element)* ] '}'
///     element := array | NULL | '"' escaped-content '"' | unquoted-token
/// The explicit dimension prefix (`[1:2]={...}`) is not produced by ClickHouse and is not accepted.
class PostgreSQLArrayTextParser
{
public:
    PostgreSQLArrayTextParser(std::string_view text_, const FormatSettings & settings_)
        : text(text_), settings(settings_)
    {
    }

    void parse(IColumn & column, const IDataType & type)
    {
        skipWhitespace();
        parseArray(column, type);
        skipWhitespace();
        if (pos != text.size())
            throwError("unexpected trailing characters");
    }

private:
    std::string_view text;
    const FormatSettings & settings;
    size_t pos = 0;
    /// The number of elements seen at each nesting depth. PostgreSQL multidimensional arrays are
    /// rectangular: every array at a given depth has the same number of elements as the first one
    /// completed at that depth. The write side enforces the same invariant, so a ragged literal
    /// accepted here would produce a row that this interface can no longer read back. Arrays
    /// complete innermost-first, so a depth may be filled in before its parent: the entries are
    /// optional rather than appended in order.
    std::vector<std::optional<size_t>> dimensions;
    size_t depth = 0;

    [[noreturn]] void throwError(const String & what) const
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot parse the PostgreSQL array literal {} at position {}: {}",
            quoteString(String(text)), pos, what);
    }

    void skipWhitespace()
    {
        while (pos < text.size() && isWhitespaceASCII(text[pos]))
            ++pos;
    }

    void parseArray(IColumn & column, const IDataType & type)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
        if (!array_type)
            throwError(fmt::format("an array literal cannot be read into a value of type {}", type.getName()));

        auto & array_column = assert_cast<ColumnArray &>(column);
        const auto & nested_type = array_type->getNestedType();
        IColumn & nested_column = array_column.getData();
        const size_t initial_size = nested_column.size();
        const size_t current_depth = depth;
        ++depth;
        SCOPE_EXIT({ depth = current_depth; });

        if (pos >= text.size() || text[pos] != '{')
            throwError("expected '{'");
        ++pos;

        bool first = true;
        while (true)
        {
            skipWhitespace();
            if (pos >= text.size())
                throwError("expected '}'");

            if (text[pos] == '}')
            {
                ++pos;
                break;
            }

            if (!first)
            {
                if (text[pos] != ',')
                    throwError("expected ',' or '}'");
                ++pos;
                skipWhitespace();
            }
            first = false;

            try
            {
                parseElement(nested_column, nested_type);
            }
            catch (...)
            {
                /// The element parser may have appended nothing or a partial value; leave the nested
                /// column consistent with the offsets that have not been written yet.
                nested_column.popBack(nested_column.size() - initial_size);
                throw;
            }
        }

        /// `nested_column` gains exactly one entry per element at this depth - one value for a scalar
        /// element type, one offset for a nested array - so this is the element count of this array.
        const size_t element_count = nested_column.size() - initial_size;
        if (dimensions.size() <= current_depth)
            dimensions.resize(current_depth + 1);
        if (!dimensions[current_depth].has_value())
            dimensions[current_depth] = element_count;
        else if (*dimensions[current_depth] != element_count)
        {
            /// Reject before the offsets are committed, and leave the nested column as it was.
            const size_t expected = *dimensions[current_depth];
            nested_column.popBack(element_count);
            throwError(fmt::format(
                "multidimensional PostgreSQL arrays must be rectangular, but an array of {} element(s) follows "
                "an array of {} element(s) at the same nesting depth",
                element_count, expected));
        }

        array_column.getOffsets().push_back(nested_column.size());
    }

    void parseElement(IColumn & nested_column, const DataTypePtr & nested_type)
    {
        if (isArray(nested_type))
        {
            parseArray(nested_column, *nested_type);
            return;
        }

        String value;
        bool quoted = false;

        if (pos < text.size() && text[pos] == '"')
        {
            quoted = true;
            ++pos;
            while (true)
            {
                if (pos >= text.size())
                    throwError("unterminated quoted element");
                const char c = text[pos];
                if (c == '"')
                {
                    ++pos;
                    break;
                }
                if (c == '\\')
                {
                    ++pos;
                    if (pos >= text.size())
                        throwError("unterminated escape sequence");
                }
                value += text[pos];
                ++pos;
            }
        }
        else
        {
            while (pos < text.size() && text[pos] != ',' && text[pos] != '}')
            {
                if (text[pos] == '\\')
                {
                    ++pos;
                    if (pos >= text.size())
                        throwError("unterminated escape sequence");
                }
                value += text[pos];
                ++pos;
            }

            while (!value.empty() && isWhitespaceASCII(value.back()))
                value.pop_back();
        }

        /// Only an unquoted `NULL` is a null element - `"NULL"` is the four-character string.
        if (!quoted && Poco::toUpper(value) == "NULL")
        {
            /// `LowCardinality(Nullable(T))` holds nulls too, and its default value is a null, but the
            /// column itself does not report as nullable.
            if (!isColumnNullableOrLowCardinalityNullable(nested_column))
                throwError(fmt::format("a NULL element cannot be read into a value of type {}", nested_type->getName()));
            nested_column.insertDefault();
            return;
        }

        /// The element is not a null, and what it reads as must not depend on the null representation of
        /// a text format: inside an array literal `"NULL"` and `\N` are ordinary strings, and only the
        /// array grammar decides what a null is. So the value is deserialized as the element type with
        /// its nullability stripped, and only then inserted into the element column.
        const auto value_type = removeNullable(removeLowCardinality(nested_type));
        ReadBufferFromString element_buffer(value);
        if (value_type->equals(*nested_type))
        {
            nested_type->getDefaultSerialization()->deserializeWholeText(nested_column, element_buffer, settings);
            return;
        }

        auto value_column = value_type->createColumn();
        value_type->getDefaultSerialization()->deserializeWholeText(*value_column, element_buffer, settings);
        if (auto * nullable_column = typeid_cast<ColumnNullable *>(&nested_column))
        {
            nullable_column->getNestedColumn().insertFrom(*value_column, 0);
            nullable_column->getNullMapData().push_back(false);
        }
        else
        {
            /// `LowCardinality`: the value has to go through the dictionary, so it is inserted by value.
            nested_column.insert((*value_column)[0]);
        }
    }
};

}

void readPostgreSQLArrayText(
    IColumn & column, const IDataType & type, std::string_view text, const FormatSettings & settings)
{
    PostgreSQLArrayTextParser(text, settings).parse(column, type);
}

}
