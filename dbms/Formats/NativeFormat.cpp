#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void registerInputFormatNative(FormatFactory & factory)
{
    factory.registerInputFormat("Native", [](
        ReadBuffer & buf,
        const Block & sample,
        UInt64 /* max_block_size */,
        FormatFactory::ReadCallback /* callback */,
        const FormatSettings &)
    {
        return std::make_shared<NativeBlockInputStream>(buf, sample, 0);
    });
}

void registerOutputFormatNative(FormatFactory & factory)
{
    factory.registerOutputFormat("Native", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings &)
    {
        return std::make_shared<NativeBlockOutputStream>(buf, 0, sample);
    });
}

}
