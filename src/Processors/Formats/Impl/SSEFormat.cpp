#include <Processors/Formats/Impl/SSEFormat.h>
#include "Formats/FormatSettings.h"
#include "Processors/Formats/IOutputFormat.h"
#include "Processors/Formats/Impl/JSONEachRowRowOutputFormat.h"
#include "Processors/Formats/Impl/JSONEachRowWithProgressRowOutputFormat.h"


namespace DB
{

void registerOutputFormatSSE(FormatFactory & factory)
{
    
    factory.registerOutputFormat("JSONEachRowWithProgressSSE", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & )
    {
        FormatSettings sse_settings;
        sse_settings.json.array_of_rows = false;
        sse_settings.json.pretty_print = false;
        sse_settings.json.escape_forward_slashes = true;
        sse_settings.json.quote_64bit_integers = true;
        sse_settings.json.quote_64bit_floats = true;
        sse_settings.json.serialize_as_strings = false;
        return std::make_shared<SSEFormat<JSONEachRowWithProgressRowOutputFormat, true>>(buf, sample, sse_settings);
        
    });

    factory.registerOutputFormat("JSONEachRowSSE", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & )
    {
        FormatSettings sse_settings;
        sse_settings.json.array_of_rows = false;
        sse_settings.json.pretty_print = false;
        sse_settings.json.escape_forward_slashes = true;
        sse_settings.json.quote_64bit_integers = true;
        sse_settings.json.quote_64bit_floats = true;
        sse_settings.json.serialize_as_strings = false;
        return std::make_shared<SSEFormat<JSONEachRowRowOutputFormat, false>>(buf, sample, sse_settings);
    });
}

}
