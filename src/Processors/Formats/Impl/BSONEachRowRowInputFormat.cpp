#include <IO/ReadBufferFromString.h>

#include <Processors/Formats/Impl/BSONEachRowRowInputFormat.h>
#include <Processors/Formats/Impl/BSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

}

BSONEachRowRowInputFormat::BSONEachRowRowInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    Params params_,
    const FormatSettings & format_settings_,
    bool yield_strings_)
    : IRowInputFormat(header_, in_, std::move(params_)), format_settings(format_settings_), name_map(header_.columns()), yield_strings(yield_strings_)
{
    size_t num_columns = getPort().getHeader().columns();

    for (size_t i = 0; i < num_columns; ++i)
    {
        const String & column_name = columnName(i);
        name_map[column_name] = i;
    }

    prev_positions.resize(num_columns);
}

const String & BSONEachRowRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t BSONEachRowRowInputFormat::columnIndex(const StringRef & name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index
        && prev_positions[key_index]
        && name == prev_positions[key_index]->getKey())
    {
        return prev_positions[key_index]->getMapped();
    }
    else
    {
        auto * it = name_map.find(name);

        if (it)
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;

            return it->getMapped();
        }
        else
            return UNKNOWN_FIELD;
    }
}

StringRef BSONEachRowRowInputFormat::readColumnName(ReadBuffer & buf)
{
    // This is just an optimization: try to avoid copying the name into current_column_name

    if (nested_prefix_length == 0 && !buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<0>(buf.position() + 1, buf.buffer().end());
        

        if (next_pos != buf.buffer().end())
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    return current_column_name;
}



void BSONEachRowRowInputFormat::readBSONObject(MutableColumns & columns)
{
    UInt32 obj_size;
    {
        union {
            char buf[4];
            UInt32 size;
        } read_value;
        in->read(read_value.buf, 4);
        obj_size = read_value.size;
    }
    UInt32 already_read = BSON_32;
    for (size_t key_index = 0; already_read + 1 < obj_size; ++key_index) {
        char type;
        in->read(type);
        StringRef name_ref = readColumnName(*in);
        const size_t column_index = columnIndex(name_ref, key_index);

        if (unlikely(ssize_t(column_index) < 0))
        {
            /// name_ref may point directly to the input buffer
            /// and input buffer may be filled with new data on next read
            /// If we want to use name_ref after another reads from buffer, we must copy it to temporary string.

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            // if (column_index == UNKNOWN_FIELD)
            //     skipUnknownField(name_ref);
            // else if (column_index == NESTED_FIELD)
            //     readNestedData(name_ref.toString(), columns);
            // else
            //     throw Exception("Logical error: illegal value of column_index", ErrorCodes::LOGICAL_ERROR);
        }
        if (seen_columns[column_index])
            throw Exception("Duplicate field found while parsing BSONEachRow format: " + columnName(column_index), ErrorCodes::INCORRECT_DATA);

        seen_columns[column_index] = true;
        read_columns[column_index] = BSONUtils::readField(*in, *columns[column_index], type, already_read);
        already_read += sizeof(type) + name_ref.size + 1;
    }
    char eof_check;
    in->read(eof_check);
    ++already_read;
    if (eof_check != 0) throw Exception("Wrong BSON syntax", ErrorCodes::INCORRECT_DATA);
}

bool BSONEachRowRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    nested_prefix_length = 0;

    if (in->eof() || in->buffer().size() == 0)
        return false;

    // LOG_DEBUG(&Poco::Logger::get("<readRow>"), "Reading new object");
    readBSONObject(columns);

    const auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);
    
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

void registerInputFormatBSONEachRow(FormatFactory & factory)
{
    factory.registerInputFormat("BSONEachRow", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
        {
            return std::make_shared<BSONEachRowRowInputFormat>(buf, sample, std::move(params), settings, false);
        }
    );
}

}
