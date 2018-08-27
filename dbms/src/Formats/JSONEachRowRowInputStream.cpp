#include <IO/ReadHelpers.h>

#include <Formats/JSONEachRowRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}


JSONEachRowRowInputStream::JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & format_settings)
    : istr(istr_), header(header_), format_settings(format_settings), name_map(header.columns())
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);

    size_t num_columns = header.columns();
    for (size_t i = 0; i < num_columns; ++i)
        name_map[header.safeGetByPosition(i).name] = i;        /// NOTE You could place names more cache-locally.
}


/** Read the field name in JSON format.
  * A reference to the field name will be written to ref.
  * You can also use temporary `tmp` buffer to copy field name there.
  */
static StringRef readName(ReadBuffer & buf, String & tmp)
{
    if (buf.position() + 1 < buf.buffer().end())
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

    readJSONString(tmp, buf);
    return tmp;
}


static void skipColonDelimeter(ReadBuffer & istr)
{
    skipWhitespaceIfAny(istr);
    assertChar(':', istr);
    skipWhitespaceIfAny(istr);
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

    assertChar('{', istr);

    size_t num_columns = columns.size();

    /// Set of columns for which the values were read. The rest will be filled with default values.
    /// TODO Ability to provide your DEFAULTs.
    bool read_columns[num_columns];
    memset(read_columns, 0, num_columns);

    bool first = true;
    while (true)
    {
        skipWhitespaceIfAny(istr);

        if (istr.eof())
            throw Exception("Unexpected end of stream while parsing JSONEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
        else if (*istr.position() == '}')
        {
            ++istr.position();
            break;
        }

        if (first)
            first = false;
        else
        {
            assertChar(',', istr);
            skipWhitespaceIfAny(istr);
        }

        StringRef name_ref = readName(istr, name_buf);

        /// NOTE Optimization is possible by caching the order of fields (which is almost always the same)
        /// and a quick check to match the next expected field, instead of searching the hash table.

        auto it = name_map.find(name_ref);
        if (name_map.end() == it)
        {
            if (!format_settings.skip_unknown_fields)
                throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

            skipColonDelimeter(istr);
            skipJSONField(istr, name_ref);
            continue;
        }

        size_t index = it->second;

        if (read_columns[index])
            throw Exception("Duplicate field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

        skipColonDelimeter(istr);

        read_columns[index] = true;

        try
        {
            header.getByPosition(index).type->deserializeTextJSON(*columns[index], istr, format_settings);
        }
        catch (Exception & e)
        {
            e.addMessage("(while read the value of key " + name_ref.toString() + ")");
            throw;
        }
    }

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
