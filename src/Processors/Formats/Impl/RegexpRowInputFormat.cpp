#include <stdlib.h>
#include <base/find_symbols.h>
#include <Processors/Formats/Impl/RegexpRowInputFormat.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

RegexpRowInputFormat::RegexpRowInputFormat(
        ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
        : IRowInputFormat(header_, in_, std::move(params_))
        , buf(in_)
        , format_settings(format_settings_)
        , field_format(stringToFormat(format_settings_.regexp.escaping_rule))
        , regexp(format_settings_.regexp.regexp)
{
    size_t fields_count = regexp.NumberOfCapturingGroups();
    matched_fields.resize(fields_count);
    re2_arguments.resize(fields_count);
    re2_arguments_ptrs.resize(fields_count);
    for (size_t i = 0; i != fields_count; ++i)
    {
        // Bind an argument to a matched field.
        re2_arguments[i] = &matched_fields[i];
        // Save pointer to argument.
        re2_arguments_ptrs[i] = &re2_arguments[i];
    }
}


void RegexpRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    buf.reset();
}

RegexpRowInputFormat::ColumnFormat RegexpRowInputFormat::stringToFormat(const String & format)
{
    if (format == "Escaped")
        return ColumnFormat::Escaped;
    if (format == "Quoted")
        return ColumnFormat::Quoted;
    if (format == "CSV")
        return ColumnFormat::Csv;
    if (format == "JSON")
        return ColumnFormat::Json;
    if (format == "Raw")
        return ColumnFormat::Raw;
    throw Exception("Unsupported column format \"" + format + "\".", ErrorCodes::BAD_ARGUMENTS);
}

bool RegexpRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    const auto & type = getPort().getHeader().getByPosition(index).type;
    bool parse_as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();
    bool read = true;
    ReadBuffer field_buf(const_cast<char *>(matched_fields[index].data()), matched_fields[index].size(), 0);
    try
    {
        const auto & serialization = serializations[index];
        switch (field_format)
        {
            case ColumnFormat::Escaped:
                if (parse_as_nullable)
                    read = SerializationNullable::deserializeTextEscapedImpl(*columns[index], field_buf, format_settings, serialization);
                else
                    serialization->deserializeTextEscaped(*columns[index], field_buf, format_settings);
                break;
            case ColumnFormat::Quoted:
                if (parse_as_nullable)
                    read = SerializationNullable::deserializeTextQuotedImpl(*columns[index], field_buf, format_settings, serialization);
                else
                    serialization->deserializeTextQuoted(*columns[index], field_buf, format_settings);
                break;
            case ColumnFormat::Csv:
                if (parse_as_nullable)
                    read = SerializationNullable::deserializeTextCSVImpl(*columns[index], field_buf, format_settings, serialization);
                else
                    serialization->deserializeTextCSV(*columns[index], field_buf, format_settings);
                break;
            case ColumnFormat::Json:
                if (parse_as_nullable)
                    read = SerializationNullable::deserializeTextJSONImpl(*columns[index], field_buf, format_settings, serialization);
                else
                    serialization->deserializeTextJSON(*columns[index], field_buf, format_settings);
                break;
            case ColumnFormat::Raw:
                if (parse_as_nullable)
                    read = SerializationNullable::deserializeTextRawImpl(*columns[index], field_buf, format_settings, serialization);
                else
                    serialization->deserializeTextRaw(*columns[index], field_buf, format_settings);
                break;
            default:
                break;
        }
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of column " +  getPort().getHeader().getByPosition(index).name + ")");
        throw;
    }
    return read;
}

void RegexpRowInputFormat::readFieldsFromMatch(MutableColumns & columns, RowReadExtension & ext)
{
    if (matched_fields.size() != columns.size())
        throw Exception("The number of matched fields in line doesn't match the number of columns.", ErrorCodes::INCORRECT_DATA);

    ext.read_columns.assign(columns.size(), false);
    for (size_t columns_index = 0; columns_index < columns.size(); ++columns_index)
    {
        ext.read_columns[columns_index] = readField(columns_index, columns);
    }
}

bool RegexpRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (buf.eof())
        return false;

    PeekableReadBufferCheckpoint checkpoint{buf};

    size_t line_size = 0;

    do
    {
        char * pos = find_first_symbols<'\n', '\r'>(buf.position(), buf.buffer().end());
        line_size += pos - buf.position();
        buf.position() = pos;
    } while (buf.position() == buf.buffer().end() && !buf.eof());

    buf.makeContinuousMemoryFromCheckpointToPos();
    buf.rollbackToCheckpoint();

    bool match = RE2::FullMatchN(re2::StringPiece(buf.position(), line_size), regexp, re2_arguments_ptrs.data(), re2_arguments_ptrs.size());
    bool read_line = true;

    if (!match)
    {
        if (!format_settings.regexp.skip_unmatched)
            throw Exception("Line \"" + std::string(buf.position(), line_size) + "\" doesn't match the regexp.", ErrorCodes::INCORRECT_DATA);
        read_line = false;
    }

    if (read_line)
        readFieldsFromMatch(columns, ext);

    buf.position() += line_size;

    checkChar('\r', buf);
    if (!buf.eof() && !checkChar('\n', buf))
        throw Exception("No \\n after \\r at the end of line.", ErrorCodes::INCORRECT_DATA);

    return true;
}

void registerInputFormatRegexp(FormatFactory & factory)
{
    factory.registerInputFormat("Regexp", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<RegexpRowInputFormat>(buf, sample, std::move(params), settings);
    });
}

static std::pair<bool, size_t> fileSegmentationEngineRegexpImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n', '\r'>(pos, in.buffer().end());
        if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
        else if (pos == in.buffer().end())
            continue;

        // Support DOS-style newline ("\r\n")
        if (*pos == '\r')
        {
            ++pos;
            if (pos == in.buffer().end())
                loadAtPosition(in, memory, pos);
        }

        if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size)
            need_more_data = false;

        ++pos;
        ++number_of_rows;
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineRegexp(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("Regexp", &fileSegmentationEngineRegexpImpl);
}

}
