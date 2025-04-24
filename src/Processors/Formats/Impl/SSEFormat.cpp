#include <Processors/Formats/Impl/SSEFormat.h>
#include "Formats/FormatSettings.h"
#include "Processors/Formats/IOutputFormat.h"


namespace DB
{

void registerOutputFormatSSE(FormatFactory & factory)
{

    auto register_function = [&](const String & format)
    {
        factory.registerOutputFormat(format, [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & fs_)
        {

            FormatSettings settings = fs_;
            settings.json = {};
            settings.json.serialize_as_strings = false;
            return std::make_shared<SSEFormat>(buf, sample, settings);
        });

        // factory.markOutputFormatSupportsParallelFormatting(format);
    };

    register_function("SSEFormat");
}

}
