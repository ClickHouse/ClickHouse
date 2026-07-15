#include <Processors/Formats/Impl/PNGOutputFormat.h>

#if USE_LIBPNG && USE_BASE64

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGSerializer.h>
#include <Formats/PNGTerminalOutput.h>
#include <Formats/PNGWriter.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>

namespace DB
{

namespace
{
constexpr auto FORMAT_NAME = "PNG";

/// Encode the image as a PNG file into a memory-tracked buffer. The buffer can be large (proportional to the
/// image size), so it uses the throwing memory tracker to honor `max_memory_usage` instead of overshooting it.
StringWithMemoryTracking encodePNG(const PNGSerializer & serializer)
{
    StringWithMemoryTracking png;
    WriteBufferFromStringWithMemoryTracking png_buf(png);
    PNGWriter writer(png_buf, serializer.getWidth(), serializer.getHeight(), serializer.getChannels());
    writer.writeImage(reinterpret_cast<const unsigned char *>(serializer.getPixels()));
    writer.finalize();
    png_buf.finalize();
    return png;
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

void PNGOutputFormat::resetFormatterImpl()
{
    /// Reusable output paths (e.g. `MessageQueueSink`) finalize one image and then reuse this
    /// formatter for the next message. Clear the accumulated pixels and the implicit coordinate
    /// cursor so the next image starts from scratch instead of carrying over stale state.
    (*serializer).reset();
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
