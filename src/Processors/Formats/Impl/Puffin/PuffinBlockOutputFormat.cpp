#include <Processors/Formats/Impl/Puffin/PuffinBlockOutputFormat.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>

#include <Formats/FormatFactory.h>

namespace DB
{

PuffinBlockOutputFormat::PuffinBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings)
    : IOutputFormat(header_, out_)
    , builder(createPuffinBlobBuilder(getPort(PortKind::Main).getHeader(), format_settings))
{
}

void PuffinBlockOutputFormat::consume(Chunk chunk)
{
    builder->consume(chunk);
}

void PuffinBlockOutputFormat::finalizeImpl()
{
    PuffinWriter writer(out);
    builder->write(writer);
    writer.finalize();
}

void PuffinBlockOutputFormat::resetFormatterImpl()
{
    builder->clear();
}

void registerOutputFormatPuffin(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Puffin",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings, FormatFilterInfoPtr)
        { return std::make_shared<PuffinBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings); });
    factory.markFormatHasNoAppendSupport("Puffin");
    factory.markOutputFormatNotTTYFriendly("Puffin");
    factory.setContentType("Puffin", "application/octet-stream");
}

}
