#include <DataTypes/Serializations/ISerialization.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/TSKVRowOutputFormat.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>

namespace DB
{

TSKVRowOutputFormat::TSKVRowOutputFormat(WriteBuffer & out_, SharedHeader header, const FormatSettings & format_settings_)
    : TabSeparatedRowOutputFormat(out_, header, false, false, false, format_settings_), fields(header->getNamesAndTypes())
{
    for (auto & field : fields)
    {
        WriteBufferFromOwnString wb;
        writeAnyEscapedString<'='>(field.name.data(), field.name.data() + field.name.size(), wb);
        writeCString("=", wb);
        field.name = wb.str();
    }
}


void TSKVRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    writeString(fields[field_number].name, out);
    serialization.serializeTextEscaped(column, row_num, out, format_settings);
    ++field_number;
}


void TSKVRowOutputFormat::writeRowEndDelimiter()
{
    writeChar('\n', out);
    field_number = 0;
}


void registerOutputFormatTSKV(FormatFactory & factory);
void registerOutputFormatTSKV(FormatFactory & factory)
{
    factory.registerOutputFormat("TSKV", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<TSKVRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("TSKV");
    factory.setContentType("TSKV", "text/tab-separated-values; charset=UTF-8");

    /// `TSKV` always writes the column names into the header (`writeAnyEscapedString<'='>`, which
    /// escapes control characters but does not validate UTF-8), so a column name that is not valid
    /// UTF-8 (a quoted identifier with arbitrary bytes) makes the output non-textual. It is knowable
    /// from the header, so the text framings reject or base64-encode the output accordingly.
    factory.registerOutputFormatMayProduceRawBytesChecker("TSKV", [](const FormatSettings &, const Block & header)
    {
        return headerNamesMayProduceRawBytes(header, /*with_names=*/true, /*with_types=*/false);
    });
}

}
