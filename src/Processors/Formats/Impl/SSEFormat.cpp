#include <Processors/Formats/Impl/SSEFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>


namespace DB
{

void registerOutputFormatSSE(FormatFactory & factory)
{

    factory.registerOutputFormat("JSONEachRowWithProgressEventStream", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings &)
    {
        FormatSettings sse_settings;
        sse_settings.json.array_of_rows = false;
        sse_settings.json.pretty_print = false;
        sse_settings.json.escape_forward_slashes = true;
        sse_settings.json.quote_64bit_integers = true;
        sse_settings.json.quote_64bit_floats = true;
        sse_settings.json.serialize_as_strings = false;
        return std::make_shared<SSEFormatJSONWithProgress>(buf, sample, sse_settings);
    });

    factory.registerOutputFormat("JSONEachRowEventStream", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings &)
    {
        FormatSettings sse_settings;
        sse_settings.json.array_of_rows = false;
        sse_settings.json.pretty_print = false;
        sse_settings.json.escape_forward_slashes = true;
        sse_settings.json.quote_64bit_integers = true;
        sse_settings.json.quote_64bit_floats = true;
        sse_settings.json.serialize_as_strings = false;
        return std::make_shared<SSEFormatJSON>(buf, sample, sse_settings);
    });


    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerOutputFormat(format_name, [with_names, with_types](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings)
        {
            return std::make_shared<SSEFormatCSV>(buf, sample, with_names, with_types, format_settings);
        });
        factory.markOutputFormatSupportsParallelFormatting(format_name);
    };

    register_func("CSVEventStream", false, false);
    register_func("CSVWithNamesEventStream", true, false);
    register_func("CSVWithNamesAndTypesEventStream", true, true);
}

}
