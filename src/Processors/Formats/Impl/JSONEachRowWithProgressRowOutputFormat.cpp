#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":{", out);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", out);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    writeCString("{\"progress\":", out);
    progress.writeJSON(out);
    writeCString("}\n", out);
}


void registerOutputFormatProcessorJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSONEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            FormatFactory::WriteCallback callback,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, callback, format_settings);
    });
}

}
