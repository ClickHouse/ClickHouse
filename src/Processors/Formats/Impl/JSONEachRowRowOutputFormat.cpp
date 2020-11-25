#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_, callback), settings(settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    fields.resize(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString buf(fields[i]);
        writeJSONString(sample.getByPosition(i).name, buf, settings);
    }
}


void JSONEachRowRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeString(fields[field_number], out);
    writeChar(':', out);
    type.serializeAsTextJSON(column, row_num, out, settings);
    ++field_number;
}


void JSONEachRowRowOutputFormat::writeFieldDelimiter()
{
    writeChar(',', out);
}


void JSONEachRowRowOutputFormat::writeRowStartDelimiter()
{
    writeChar('{', out);
}


void JSONEachRowRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}\n", out);
    field_number = 0;
}


void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONEachRow", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONEachRowRowOutputFormat>(buf, sample, callback, format_settings);
    });
}

}
