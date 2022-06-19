#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/TSKVRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

TSKVRowOutputFormat::TSKVRowOutputFormat(WriteBuffer & out_, const Block & header, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : TabSeparatedRowOutputFormat(out_, header, false, false, false, params_, format_settings_), fields(header.getNamesAndTypes())
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


void registerOutputFormatTSKV(FormatFactory & factory)
{
    factory.registerOutputFormat("TSKV", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<TSKVRowOutputFormat>(buf, sample, params, settings);
    });
    factory.markOutputFormatSupportsParallelFormatting("TSKV");
}

}
