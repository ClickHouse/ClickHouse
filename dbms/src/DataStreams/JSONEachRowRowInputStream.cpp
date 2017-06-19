#include <IO/ReadHelpers.h>
#include <DataStreams/JSONEachRowRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}


JSONEachRowRowInputStream::JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & sample_, bool skip_unknown_)
    : istr(istr_), sample(sample_), skip_unknown(skip_unknown_), name_map(sample.columns())
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(istr);

    size_t columns = sample.columns();
    for (size_t i = 0; i < columns; ++i)
        name_map[sample.safeGetByPosition(i).name] = i;        /// NOTE You could place names more cache-locally.
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


bool JSONEachRowRowInputStream::read(Block & block)
{
    skipWhitespaceIfAny(istr);
    if (istr.eof())
        return false;

    assertChar('{', istr);

    size_t columns = block.columns();

    /// Set of columns for which the values were read. The rest will be filled with default values.
    /// TODO Ability to provide your DEFAULTs.
    bool read_columns[columns];
    memset(read_columns, 0, columns);

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
            if (!skip_unknown)
                throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

            skipColonDelimeter(istr);
            skipJSONFieldPlain(istr, name_ref);
            continue;
        }

        size_t index = it->second;

        if (read_columns[index])
            throw Exception("Duplicate field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

        skipColonDelimeter(istr);

        read_columns[index] = true;

        auto & col = block.getByPosition(index);
        col.type.get()->deserializeTextJSON(*col.column.get(), istr);
    }

    skipWhitespaceIfAny(istr);
    if (!istr.eof() && (*istr.position() == ',' || *istr.position() == ';'))    /// Semicolon is added for convenience as it could be used at end of INSERT query.
        ++istr.position();

    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < columns; ++i)
        if (!read_columns[i])
            block.getByPosition(i).column.get()->insertDefault();

    return true;
}


void JSONEachRowRowInputStream::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(istr);
}

}
