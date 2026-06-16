#include <Processors/Formats/Impl/HiveTextRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{


HiveTextRowOutputFormat::HiveTextRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_)
{
}

void HiveTextRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextHive(column, row_num, out, format_settings);
}

void HiveTextRowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.hive_text.fields_delimiter, out);
}

void HiveTextRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(format_settings.hive_text.rows_delimiter, out);
}

void registerOutputFormatHiveText(FormatFactory & factory);
void registerOutputFormatHiveText(FormatFactory & factory)
{
    factory.registerOutputFormat("HiveText", [](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings,
                   FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<HiveTextRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
        });
    factory.markOutputFormatSupportsParallelFormatting("HiveText");
}

}
