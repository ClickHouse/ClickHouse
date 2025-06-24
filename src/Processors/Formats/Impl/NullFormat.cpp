#include <Processors/Formats/Impl/NullFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>


namespace DB
{

WriteBufferFromPointer NullOutputFormat::empty_buffer(nullptr, 0);

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
