#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <Common/assert_cast.h>

#if USE_HIVE

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


static FormatSettings updateFormatSettings(const FormatSettings & settings, const Block & header)
{
    FormatSettings updated = settings;
    updated.skip_unknown_fields = true;
    updated.with_names_use_header = true;
    updated.date_time_input_format = FormatSettings::DateTimeInputFormat::BestEffort;
    updated.defaults_for_omitted_fields = true;
    updated.csv.delimiter = updated.hive_text.fields_delimiter;
    updated.csv.allow_variable_number_of_columns = settings.hive_text.allow_variable_number_of_columns;
    if (settings.hive_text.input_field_names.empty())
        updated.hive_text.input_field_names = header.getNames();
    return updated;
}

HiveTextRowInputFormat::HiveTextRowInputFormat(
    const Block & header_, ReadBuffer & in_, const Params & params_, const FormatSettings & format_settings_)
    : HiveTextRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, updateFormatSettings(format_settings_, header_))
{
}

HiveTextRowInputFormat::HiveTextRowInputFormat(
    const Block & header_, std::shared_ptr<PeekableReadBuffer> buf_, const Params & params_, const FormatSettings & format_settings_)
    : CSVRowInputFormat(
        header_, buf_, params_, true, false, format_settings_, std::make_unique<HiveTextFormatReader>(*buf_, format_settings_))
{
}

HiveTextFormatReader::HiveTextFormatReader(PeekableReadBuffer & buf_, const FormatSettings & format_settings_)
    : CSVFormatReader(buf_, format_settings_), input_field_names(format_settings_.hive_text.input_field_names)
{
}

std::vector<String> HiveTextFormatReader::readNames()
{
    return input_field_names;
}

std::vector<String> HiveTextFormatReader::readTypes()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "HiveTextRowInputFormat::readTypes is not implemented");
}

void registerInputFormatHiveText(FormatFactory & factory)
{
    factory.registerInputFormat(
        "HiveText", [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        {
            return std::make_shared<HiveTextRowInputFormat>(sample, buf, params, settings);
        });
}

void registerFileSegmentationEngineHiveText(FormatFactory & factory)
{
    factory.registerFileSegmentationEngineCreator(
        "HiveText",
        [](const FormatSettings & settings) -> FormatFactory::FileSegmentationEngine {
            return [settings] (ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
            {
                return fileSegmentationEngineCSVImpl(in, memory, min_bytes, 0, max_rows, settings);
            };
        });
}

}
#endif
