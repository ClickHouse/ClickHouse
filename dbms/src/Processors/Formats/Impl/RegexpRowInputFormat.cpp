#include <string>
#include <common/find_symbols.h>
#include <Processors/Formats/Impl/RegexpRowInputFormat.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int BAD_ARGUMENTS;
}

RegexpRowInputFormat::RegexpRowInputFormat(
        ReadBuffer & in_, const Block & header_, Params params_,  const FormatSettings & format_settings_)
        : IRowInputFormat(header_, in_, std::move(params_)), format_settings(format_settings_), regexp(format_settings_.regexp.regexp)
{
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
    ReadBuffer field_buf(matched_fields[index + 1].first, matched_fields[index + 1].length(), 0);
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
            default:
                __builtin_unreachable();
        }
    }
    catch (Exception & e)
    {
        throw;
    }
    return read;
}

void RegexpRowInputFormat::readFieldsFromMatch(MutableColumns & columns, RowReadExtension & ext)
{
    if (matched_fields.size() != columns.size() + 1)
        throw Exception("The number of matched fields in line doesn't match the number of columns.", ErrorCodes::INCORRECT_DATA);

    ext.read_columns.assign(columns.size(), false);
    for (size_t columns_index = 0; columns_index < columns.size(); ++columns_index)
    {
        ext.read_columns[columns_index] = readField(columns_index, columns);
    }
}

bool RegexpRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in.eof())
        return false;

    char * line_end = find_first_symbols<'\n', '\r'>(in.position(), in.buffer().end());
    bool match = std::regex_match(in.position(), line_end, matched_fields, regexp);

    if (!match)
    {
        if (!format_settings.regexp.skip_unmatched)
            throw Exception("Line \"" + std::string(in.position(), line_end) + "\" doesn't match the regexp.", ErrorCodes::INCORRECT_DATA);
        in.position() = line_end + 1;
        return true;
    }

    readFieldsFromMatch(columns, ext);

    in.position() = line_end + 1;
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
    bool need_more_data = true;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n', '\r'>(pos, in.buffer().end());
        if (pos == in.buffer().end())
            continue;

        if (memory.size() + static_cast<size_t>(pos - in.position()) >= min_chunk_size)
            need_more_data = false;
        ++pos;
    }

    saveUpToPosition(in, memory, pos);

    return loadAtPosition(in, memory, pos);
}

void registerFileSegmentationEngineRegexp(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("Regexp", &fileSegmentationEngineRegexpImpl);
}

}
