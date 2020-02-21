#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{


void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCHCString("{\"row\":{", out);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCHCString("}}\n", out);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    writeCHCString("{\"progress\":", out);
    progress.writeJSON(out);
    writeCHCString("}\n", out);
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
