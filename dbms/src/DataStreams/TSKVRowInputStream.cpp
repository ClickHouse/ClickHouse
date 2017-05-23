#include <IO/ReadHelpers.h>
#include <DataStreams/TSKVRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}


TSKVRowInputStream::TSKVRowInputStream(ReadBuffer & istr_, const Block & sample_, bool skip_unknown_)
    : istr(istr_), sample(sample_), skip_unknown(skip_unknown_), name_map(sample.columns())
{
    /// In this format, we assume that column name cannot contain BOM,
    ///  so BOM at beginning of stream cannot be confused with name of field, and it is safe to skip it.
    skipBOMIfExists(istr);

    size_t columns = sample.columns();
    for (size_t i = 0; i < columns; ++i)
        name_map[sample.safeGetByPosition(i).name] = i;        /// NOTE You could place names more cache-locally.
}


/** Read the field name in the `tskv` format.
  * Return true if the field is followed by an equal sign,
  *  otherwise (field with no value) return false.
  * The reference to the field name will be written to `ref`.
  * A temporary `tmp` buffer can also be used to copy the field name to it.
  * When reading, skips the name and the equal sign after it.
  */
static bool readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
    tmp.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\t', '\n', '\\', '='>(buf.position(), buf.buffer().end());

        if (next_pos == buf.buffer().end())
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.next();
            continue;
        }

        /// Came to the end of the name.
        if (*next_pos != '\\')
        {
            bool have_value = *next_pos == '=';
            if (tmp.empty())
            {
                /// No need to copy data, you can refer directly to the `buf`.
                ref = StringRef(buf.position(), next_pos - buf.position());
                buf.position() += next_pos + have_value - buf.position();
            }
            else
            {
                /// Copy the data to a temporary string and return a reference to it.
                tmp.append(buf.position(), next_pos - buf.position());
                buf.position() += next_pos + have_value - buf.position();
                ref = StringRef(tmp);
            }
            return have_value;
        }
        /// The name has an escape sequence.
        else
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.position() += next_pos + 1 - buf.position();
            if (buf.eof())
                throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

            tmp.push_back(parseEscapeSequence(*buf.position()));
            ++buf.position();
            continue;
        }
    }

    throw Exception("Unexpected end of stream while reading key name from TSKV format", ErrorCodes::CANNOT_READ_ALL_DATA);
}


bool TSKVRowInputStream::read(Block & block)
{
    if (istr.eof())
        return false;

    size_t columns = block.columns();

    /// Set of columns for which the values were read. The rest will be filled with default values.
    /// TODO Ability to provide your DEFAULTs.
    bool read_columns[columns];
    memset(read_columns, 0, columns);

    if (unlikely(*istr.position() == '\n'))
    {
        /// An empty string. It is permissible, but it is unclear why.
        ++istr.position();
    }
    else
    {
        while (true)
        {
            StringRef name_ref;
            bool has_value = readName(istr, name_ref, name_buf);
            ssize_t index = -1;

            if (has_value)
            {
                /// NOTE Optimization is possible by caching the order of fields (which is almost always the same)
                /// and quickly checking for the next expected field, instead of searching the hash table.

                auto it = name_map.find(name_ref);
                if (name_map.end() == it)
                {
                    if (!skip_unknown)
                        throw Exception("Unknown field found while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

                    /// If the key is not found, skip the value.
                    NullSink sink;
                    readEscapedStringInto(sink, istr);
                }
                else
                {
                    index = it->second;

                    if (read_columns[index])
                        throw Exception("Duplicate field found while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

                    read_columns[index] = true;

                    auto & col = block.getByPosition(index);
                    col.type.get()->deserializeTextEscaped(*col.column.get(), istr);
                }
            }
            else
            {
                /// The only thing that can go without value is `tskv` fragment that is ignored.
                if (!(name_ref.size == 4 && 0 == memcmp(name_ref.data, "tskv", 4)))
                    throw Exception("Found field without value while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);
            }

            if (istr.eof())
            {
                throw Exception("Unexpected end of stream after field in TSKV format: " + name_ref.toString(), ErrorCodes::CANNOT_READ_ALL_DATA);
            }
            else if (*istr.position() == '\t')
            {
                ++istr.position();
                continue;
            }
            else if (*istr.position() == '\n')
            {
                ++istr.position();
                break;
            }
            else
            {
                /// Possibly a garbage was written into column, remove it
                if (index >= 0)
                {
                    block.getByPosition(index).column->popBack(1);
                    read_columns[index] = false;
                }

                throw Exception("Found garbage after field in TSKV format: " + name_ref.toString(), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
            }
        }
    }

    /// Fill in the not met columns with default values.
    for (size_t i = 0; i < columns; ++i)
        if (!read_columns[i])
            block.getByPosition(i).column.get()->insertDefault();

    return true;
}


void TSKVRowInputStream::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(istr);
}

}
