#include <IO/ReadHelpers.h>

#include <Formats/JSONEachRowRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

} // unnamed namespace


JSONEachRowRowInputStream::JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & format_settings)
    : istr(istr_), header(header_), format_settings(format_settings), name_map(header.columns())
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);

    size_t num_columns = header.columns();
    for (size_t i = 0; i < num_columns; ++i)
    {
        const String& colname = columnName(i);
        name_map[colname] = i;        /// NOTE You could place names more cache-locally.
        if (format_settings.import_nested_json)
        {
            const auto splitted = Nested::splitName(colname);
            if (!splitted.second.empty())
            {
                const StringRef table_name(colname.data(), splitted.first.size());
                name_map[table_name] = NESTED_FIELD;
            }
        }
    }
}

const String& JSONEachRowRowInputStream::columnName(size_t i) const
{
    return header.safeGetByPosition(i).name;
}

size_t JSONEachRowRowInputStream::columnIndex(const StringRef& name) const
{
    /// NOTE Optimization is possible by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    const auto it = name_map.find(name);
    return name_map.end() == it ? UNKNOWN_FIELD : it->second;
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  */
StringRef JSONEachRowRowInputStream::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name
    if (nested_prefix_length == 0 && buf.position() + 1 < buf.buffer().end())
    {
        const char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() += next_pos - buf.position();
            assertChar('"', buf);
            return res;
        }
    }

    current_column_name.resize(nested_prefix_length);
    readJSONStringInto(current_column_name, buf);
    return current_column_name;
}


static void skipColonDelimeter(ReadBuffer & istr)
{
    skipWhitespaceIfAny(istr);
    assertChar(':', istr);
    skipWhitespaceIfAny(istr);
}

void JSONEachRowRowInputStream::skipUnknownField(const StringRef & name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

    skipJSONField(istr, name_ref);
}

void JSONEachRowRowInputStream::readField(size_t index, MutableColumns & columns)
{
    if (read_columns[index])
        throw Exception("Duplicate field found while parsing JSONEachRow format: " + columnName(index), ErrorCodes::INCORRECT_DATA);

    try
    {
        header.getByPosition(index).type->deserializeTextJSON(*columns[index], istr, format_settings);
    }
    catch (Exception & e)
    {
        e.addMessage("(while read the value of key " + columnName(index) + ")");
        throw;
    }

    read_columns[index] = true;
}

bool JSONEachRowRowInputStream::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(istr);

    if (istr.eof())
        throw Exception("Unexpected end of stream while parsing JSONEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
    else if (*istr.position() == '}')
    {
        ++istr.position();
        return false;
    }

    if (key_index > 0)
    {
        assertChar(',', istr);
        skipWhitespaceIfAny(istr);
    }
    return true;
}

void JSONEachRowRowInputStream::readJSONObject(MutableColumns & columns)
{
    assertChar('{', istr);

    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(istr);

        skipColonDelimeter(istr);

        const size_t column_index = columnIndex(name_ref);
        if (column_index == UNKNOWN_FIELD)
            skipUnknownField(name_ref);
        else if (column_index == NESTED_FIELD)
            readNestedData(name_ref.toString(), columns);
        else
            readField(column_index, columns);
    }
}

void JSONEachRowRowInputStream::readNestedData(const String & name, MutableColumns & columns)
{
    current_column_name = name;
    current_column_name.push_back('.');
    nested_prefix_length = current_column_name.size();
    readJSONObject(columns);
    nested_prefix_length = 0;
}

bool JSONEachRowRowInputStream::read(MutableColumns & columns)
{
    skipWhitespaceIfAny(istr);

    /// We consume ;, or \n before scanning a new row, instead scanning to next row at the end.
    /// The reason is that if we want an exact number of rows read with LIMIT x
    /// from a streaming table engine with text data format, like File or Kafka
    /// then seeking to next ;, or \n would trigger reading of an extra row at the end.

    /// Semicolon is added for convenience as it could be used at end of INSERT query.
    if (!istr.eof() && (*istr.position() == ',' || *istr.position() == ';'))
        ++istr.position();

    skipWhitespaceIfAny(istr);
    if (istr.eof())
        return false;

    size_t num_columns = columns.size();

    /// Set of columns for which the values were read. The rest will be filled with default values.
    /// TODO Ability to provide your DEFAULTs.
    read_columns.assign(num_columns, false);

    nested_prefix_length = 0;
    readJSONObject(columns);

    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!read_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    return true;
}


void JSONEachRowRowInputStream::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(istr);
}


void registerInputFormatJSONEachRow(FormatFactory & factory)
{
    factory.registerInputFormat("JSONEachRow", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context &,
        size_t max_block_size,
        const FormatSettings & settings)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::make_shared<JSONEachRowRowInputStream>(buf, sample, settings),
            sample, max_block_size, settings);
    });
}

}
