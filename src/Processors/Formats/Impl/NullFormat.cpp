#include <Formats/FormatFactory.h>
#include <IO/NullWriteBuffer.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/Port.h>

namespace DB
{

NullWriteBuffer NullOutputFormat::empty_buffer;

NullOutputFormat::NullOutputFormat(SharedHeader header) : IOutputFormat(header, empty_buffer) {}

void registerOutputFormatNull(FormatFactory & factory)
{
    factory.registerOutputFormat("Null", [](
        WriteBuffer &,
        const Block & sample,
        const FormatSettings &,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<NullOutputFormat>(std::make_shared<const Block>(sample));
    });
}

}
