#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void JSONEachRowWithProgressRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("{\"row\":{", *ostr);
}

void JSONEachRowWithProgressRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("}}\n", *ostr);
    field_number = 0;
}

void JSONEachRowWithProgressRowOutputFormat::writeProgress(const Progress & value)
{
    writeCString("{\"progress\":", *ostr);
    value.writeJSON(*ostr);
    writeCString("}\n", *ostr);
}

void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = false;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });

    factory.registerOutputFormat("JSONStringsEachRowWithProgress", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & _format_settings)
    {
        FormatSettings settings = _format_settings;
        settings.json.serialize_as_strings = true;
        return std::make_shared<JSONEachRowWithProgressRowOutputFormat>(buf, sample, settings);
    });
}

}
