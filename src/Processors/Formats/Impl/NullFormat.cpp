#include <Processors/Formats/Impl/NullFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/NullWriteBuffer.h>


namespace DB
{

NullWriteBuffer NullOutputFormat::empty_buffer;

NullOutputFormat::NullOutputFormat(const Block & header) : IOutputFormat(header, empty_buffer) {}

void registerOutputFormatNull(FormatFactory & factory)
{
    factory.registerOutputFormat("Null", [](
        WriteBuffer &,
        const Block & sample,
        const FormatSettings &)
    {
        return std::make_shared<NullOutputFormat>(sample);
    });
}

}
