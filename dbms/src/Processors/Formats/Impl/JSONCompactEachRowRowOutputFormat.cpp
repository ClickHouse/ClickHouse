#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONCompactEachRowRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


    JSONCompactEachRowRowOutputFormat::JSONCompactEachRowRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
            : IRowOutputFormat(header_, out_, callback), settings(settings_) {}


    void JSONCompactEachRowRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
    {
        type.serializeAsTextJSON(column, row_num, out, settings);
    }


    void JSONCompactEachRowRowOutputFormat::writeFieldDelimiter()
    {
        writeChar(',', out);
    }


    void JSONCompactEachRowRowOutputFormat::writeRowStartDelimiter()
    {
        writeChar('[', out);
    }


    void JSONCompactEachRowRowOutputFormat::writeRowEndDelimiter()
    {
        writeCString("]\n", out);
    }


    void registerOutputFormatProcessorJSONCompactEachRow(FormatFactory & factory)
    {
        factory.registerOutputFormatProcessor("JSONEachRow", [](
                WriteBuffer & buf,
                const Block & sample,
                const Context &,
                FormatFactory::WriteCallback callback,
                const FormatSettings & format_settings)
        {
            return std::make_shared<JSONCompactEachRowRowOutputFormat>(buf, sample, callback, format_settings);
        });
    }

}
