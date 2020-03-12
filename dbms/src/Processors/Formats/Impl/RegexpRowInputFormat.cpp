#include <stdlib.h>
#include <common/find_symbols.h>
#include <Processors/Formats/Impl/RegexpRowInputFormat.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <re2/stringpiece.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

RegexpRowInputFormat::RegexpRowInputFormat(
        ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
        : IRowInputFormat(header_, in_, std::move(params_)), buf(in_), format_settings(format_settings_), regexp(format_settings_.regexp.regexp)
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

    field_format = stringToFormat(format_settings_.regexp.escaping_rule);
}

RegexpRowInputFormat::FieldFormat RegexpRowInputFormat::stringToFormat(const String & format)
{
    if (format == "Escaped")
        return FieldFormat::Escaped;
    if (format == "Quoted")
        return FieldFormat::Quoted;
    if (format == "CSV")
        return FieldFormat::Csv;
    if (format == "JSON")
        return FieldFormat::Json;
    throw Exception("Unknown field format \"" + format + "\".", ErrorCodes::BAD_ARGUMENTS);
}

bool RegexpRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    const auto & type = getPort().getHeader().getByPosition(index).type;
    bool parse_as_nullable = format_settings.null_as_default && !type->isNullable();
    bool read = true;
    ReadBuffer field_buf(matched_fields[index].data(), matched_fields[index].size(), 0);
    try
    {
        switch (field_format)
        {
            case FieldFormat::Escaped:
                if (parse_as_nullable)
                    read = DataTypeNullable::deserializeTextEscaped(*columns[index], field_buf, format_settings, type);
                else
                    type->deserializeAsTextEscaped(*columns[index], field_buf, format_settings);
                break;
            case FieldFormat::Quoted:
                if (parse_as_nullable)
                    read = DataTypeNullable::deserializeTextQuoted(*columns[index], field_buf, format_settings, type);
                else
                    type->deserializeAsTextQuoted(*columns[index], field_buf, format_settings);
                break;
            case FieldFormat::Csv:
                if (parse_as_nullable)
                    read = DataTypeNullable::deserializeTextCSV(*columns[index], field_buf, format_settings, type);
                else
                    type->deserializeAsTextCSV(*columns[index], field_buf, format_settings);
                break;
            case FieldFormat::Json:
                if (parse_as_nullable)
                    read = DataTypeNullable::deserializeTextJSON(*columns[index], field_buf, format_settings, type);
                else
                    type->deserializeAsTextJSON(*columns[index], field_buf, format_settings);
                break;
        }
    }
    catch (Exception & e)
    {
        e.addMessage("(while read the value of key " +  getPort().getHeader().getByPosition(index).name + ")");
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

    while (!buf.eof() && *buf.position() != '\n' && *buf.position() != '\r')
    {
        ++buf.position();
        ++line_size;
    }

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

    // Two sequential increments are needed to support DOS-style newline ("\r\n").
    if (!buf.eof() && *buf.position() == '\r')
        ++buf.position();

    if (!buf.eof() && *buf.position() == '\n')
        ++buf.position();

    return true;
}

void registerInputFormatProcessorRegexp(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Regexp", [](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
    {
        return std::make_shared<RegexpRowInputFormat>(buf, sample, std::move(params), settings);
    });
}

static bool fileSegmentationEngineRegexpImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    char * pos = in.position();

    while (loadAtPosition(in, memory, pos) && (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size))
    {
        pos = find_first_symbols<'\n', '\r'>(pos, in.buffer().end());
        if (pos == in.buffer().end())
            continue;

        // Support DOS-style newline ("\r\n")
        if (*pos++ == '\r')
        {
            if (pos == in.buffer().end())
                loadAtPosition(in, memory, pos);
            if (*pos == '\n')
                ++pos;
        }
    }

    saveUpToPosition(in, memory, pos);

    return loadAtPosition(in, memory, pos);
}

void registerFileSegmentationEngineRegexp(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("Regexp", &fileSegmentationEngineRegexpImpl);
}

}
