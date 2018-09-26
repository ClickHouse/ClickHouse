#include <Formats/JSONOneLineOutputStream.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>
#include <Formats/FormatFactory.h>

namespace DB
{

void registerOutputFormatJSONOneLine(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONOneLine", [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & format_settings)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<JSONOneLineOutputStream>(buf, sample, format_settings), sample);
    });
}

}
