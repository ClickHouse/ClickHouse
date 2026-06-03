#include <Processors/Formats/Impl/PNGOutputFormat.h>

#if USE_LIBPNG

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGSerializer.h>
#include <Formats/PNGTerminalOutput.h>
#include <Formats/PNGWriter.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace
{
constexpr auto FORMAT_NAME = "PNG";

/// Encode the image as a PNG file into a string buffer.
String encodePNG(const PNGSerializer & serializer)
{
    WriteBufferFromOwnString png_buf;
    PNGWriter writer(png_buf, serializer.getWidth(), serializer.getHeight(), serializer.getChannels());
    writer.writeImage(reinterpret_cast<const unsigned char *>(serializer.getPixels()));
    writer.finalize();
    return png_buf.str();
}
}

PNGOutputFormat::PNGOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_)
    , format_settings(settings_)
    , serializer(std::make_unique<PNGSerializer>(*header_, settings_))
{
}

void PNGOutputFormat::consume(Chunk chunk)
{
    const auto & cols = chunk.getColumns();
    const auto num_rows = chunk.getNumRows();
    if (cols.empty() || num_rows == 0)
        return;

    serializer->setColumns(cols.data(), cols.size());
    for (size_t i = 0; i < num_rows; ++i)
        serializer->writeRow(i);
}

void PNGOutputFormat::finalizeImpl()
{
    const auto mode = parseImageTerminalMode(format_settings.image.terminal_mode, format_settings.is_writing_to_terminal);

    switch (mode)
    {
        case ImageTerminalMode::None:
        {
            PNGWriter writer(out, serializer->getWidth(), serializer->getHeight(), serializer->getChannels());
            writer.writeImage(reinterpret_cast<const unsigned char *>(serializer->getPixels()));
            writer.finalize();
            break;
        }
        case ImageTerminalMode::ITerm:
            writeImageITerm(out, encodePNG(*serializer));
            break;
        case ImageTerminalMode::Kitty:
            writeImageKitty(out, encodePNG(*serializer));
            break;
        case ImageTerminalMode::Sixel:
            writeImageSixel(out, serializer->getPixels(), serializer->getWidth(), serializer->getHeight(), serializer->getChannels());
            break;
    }
}

void registerOutputFormatPNG(FormatFactory & factory);
void registerOutputFormatPNG(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr)
        {
            return std::make_shared<PNGOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
        });
    factory.markOutputFormatNotTTYFriendly(FORMAT_NAME);
    factory.setContentType(FORMAT_NAME, "image/png");
    /// Each output is a complete, self-contained PNG datastream, so appending another image to the same file is invalid.
    factory.markFormatHasNoAppendSupport(FORMAT_NAME);
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatPNG(FormatFactory &);
void registerOutputFormatPNG(FormatFactory &)
{
}
}

#endif
