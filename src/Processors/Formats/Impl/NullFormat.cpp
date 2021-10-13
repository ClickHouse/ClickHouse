#include <Processors/Formats/Impl/NullFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>


namespace DB
{

WriteBuffer NullOutputFormat::empty_buffer(nullptr, 0);

void registerOutputFormatNull(FormatFactory & factory)
{
    factory.registerOutputFormat("Null", [](
        WriteBuffer &,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings &)
    {
        return std::make_shared<NullOutputFormat>(sample);
    });
}

}
