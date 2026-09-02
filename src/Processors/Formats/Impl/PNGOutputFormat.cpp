#include <Processors/Formats/Impl/PNGOutputFormat.h>

#if USE_SIMDUTF

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/PNGSerializer.h>
#include <Formats/PNGTerminalOutput.h>
#include <Formats/PNGWriter.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    , terminal_mode(parseImageTerminalMode(settings_.image.terminal_mode, settings_.is_writing_to_terminal))
    , serializer(std::make_unique<PNGSerializer>(*header_, settings_))
{
    if (!serializer->isAnimated())
        return;

    /// Sixel has no notion of an animation at all, and the Kitty graphics protocol has one, but it is a
    /// separate flow of per-frame commands ('a=f' and 'a=a') rather than an animated datastream: Kitty
    /// receives a single PNG payload and would display only the default image of the `APNG`, silently
    /// turning the animation into a still image.
    if (terminal_mode == ImageTerminalMode::Sixel || terminal_mode == ImageTerminalMode::Kitty)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The {} protocol cannot display an animation, but the result has a 't' column, which makes "
            "the PNG format produce one. Remove the 't' column, or choose another value of "
            "'output_format_image_terminal_mode'.",
            terminal_mode == ImageTerminalMode::Sixel ? "Sixel" : "Kitty");

    streaming = serializer->isStreamingAnimation();

    serializer->setFrameCallback([this](const UInt8 * pixels, UInt16 delay_num, UInt16 delay_den)
    {
        writeFrame(pixels, delay_num, delay_den);
    });
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

void PNGOutputFormat::writeFrame(const UInt8 * pixels, UInt16 delay_num, UInt16 delay_den)
{
    if (!animation_writer)
    {
        /// The terminal protocols carry the whole datastream as a single payload, so it has to be encoded
        /// into memory; without them the frames are appended to the output as they are produced.
        WriteBuffer * target = &out;
        if (terminal_mode != ImageTerminalMode::None)
        {
            animation_buffer_out = std::make_unique<WriteBufferFromStringWithMemoryTracking>(animation_buffer);
            target = animation_buffer_out.get();
        }

        animation_writer = std::make_unique<PNGWriter>(
            *target, serializer->getWidth(), serializer->getHeight(), serializer->getChannels());
        /// 0 plays means the animation loops forever.
        animation_writer->writeAnimationHeader(serializer->getDeclaredFrameCount(), /* num_plays = */ 0);
    }

    animation_writer->writeFrame(reinterpret_cast<const unsigned char *>(pixels), delay_num, delay_den);

    /// The point of the streaming mode is that a viewer can display the frames while the query is still
    /// running, and `IOutputFormat` only flushes after a whole chunk has been consumed, so a chunk with many
    /// distinct values of `t` would otherwise hold all of its frames back until the chunk ends. `flushImpl`
    /// rather than `flush`, because the writing mutex is already held by the caller of `consume`.
    /// With a terminal protocol the datastream is one payload and nothing can be sent early anyway.
    if (streaming && terminal_mode == ImageTerminalMode::None)
        flushImpl();
}

void PNGOutputFormat::finalizeImpl()
{
    if (serializer->isAnimated())
    {
        /// Hands over the frames that have not been written out yet, through the frame callback.
        serializer->finalizeFrames();

        animation_writer->writeEnd();
        animation_writer->finalize();

        switch (terminal_mode)
        {
            case ImageTerminalMode::None:
                break; /// Already written to `out`.
            case ImageTerminalMode::ITerm:
                animation_buffer_out->finalize();
                /// In the streaming mode `acTL` declared an upper bound, because the frame count is not
                /// known when the header is written. Here the whole datastream has been buffered anyway
                /// (the protocol carries it as a single payload), so the exact count is known before any
                /// byte of it is sent: patch it in, and the payload conforms to the specification
                /// regardless of the mode.
                animation_writer->patchDeclaredFrameCount(animation_buffer.data(), animation_buffer.size());
                writeImageITerm(out, animation_buffer);
                break;
            case ImageTerminalMode::Kitty:
            case ImageTerminalMode::Sixel:
                break; /// Rejected in the constructor: neither can display an `APNG` animation.
        }
        return;
    }

    switch (terminal_mode)
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
    /// cursor so the next image starts from scratch instead of carrying over stale state, and drop the
    /// animation datastream so the next one starts with its own header.
    (*serializer).reset();
    animation_writer.reset();
    animation_buffer_out.reset();
    /// The buffer holds the whole encoded animation, so it can be much larger than one image. `clear` would
    /// keep that capacity for the lifetime of the sink, hence the swap with an empty one, which frees it.
    StringWithMemoryTracking().swap(animation_buffer);
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
    factory.setDocumentation(FORMAT_NAME, Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

## Description {#description}

Renders the result of a query as a PNG image. This is useful as a built-in visualization tool.

The size of the output image is fixed by the settings
[`output_format_image_width`](/reference/settings/formats/output-format#output_format_image_width) and
[`output_format_image_height`](/reference/settings/formats/output-format#output_format_image_height)
(both default to 1024). Pixels that are not covered by the result are filled with black
(in `RGB` and grayscale modes) or with transparent black (in `RGBA` mode).

The color mode is determined automatically from the column names and types of the result:

| Columns                | Mode                                              |
|------------------------|---------------------------------------------------|
| `r`, `g`, `b`          | 8-bit RGB                                         |
| `r`, `g`, `b`, `a`     | 8-bit RGBA                                        |
| `v` of integer type    | 8-bit grayscale                                   |
| `v` of `Float*` type   | 8-bit grayscale (values in `[0, 1]` → `[0, 255]`) |
| `v` of `Bool` type     | Binary (rendered as 8-bit grayscale: `0` or `255`)|

Column names are matched case-insensitively. If the color mode cannot be unambiguously
determined (e.g. unknown column names, mixed `v` with `r`/`g`/`b`/`a`, or one of `r`/`g`/`b` missing),
the query throws an exception.

For pixel channels, integer values are clamped to `[0, 255]` and floating-point values
are clamped to `[0, 1]` and then scaled to `[0, 255]`.

The position of each record in the image is determined by one of two modes:

- **Implicit** (the default — when neither `x` nor `y` is present). Each record corresponds
  to a single pixel; pixels are filled in scanline order: left to right, top to bottom.
- **Explicit** (when `x` and `y` columns are present, both of integer types).
  The `x` and `y` columns give the pixel coordinates. Records with coordinates outside
  the image are silently ignored. In case of multiple records with the same coordinates,
  the last one wins (painter's algorithm).

## Example usage {#example-usage}

### Implicit coordinates (row-per-pixel), RGB {#implicit-rgb}

```sql
SELECT
    toUInt8(x * 25) AS r,
    toUInt8(y * 25) AS g,
    toUInt8((x + y) * 12) AS b
FROM
(
    SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100)
)
INTO OUTFILE 'gradient.png'
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10;
```

### Explicit coordinates, grayscale {#explicit-grayscale}

```sql
SELECT
    toInt32(x) AS x,
    toInt32(y) AS y,
    toUInt8(intensity) AS v
FROM points
INTO OUTFILE 'points.png'
FORMAT PNG
SETTINGS output_format_image_width = 512, output_format_image_height = 512;
```

## Animation {#animation}

If the result has a `t` column of an integer type, the format produces an animated PNG (`APNG`) instead of a
still image. Records are grouped into frames by the value of `t`, which is the relative time offset of the
frame. Every frame is an independent image: the canvas is empty at the start of each frame, and in the
implicit coordinate mode the cursor restarts from the top-left corner. The `t` column can be combined with
either coordinate mode.

The unit of `t` is given by
[`output_format_image_time_multiplier_seconds`](/reference/settings/formats/output-format#output_format_image_time_multiplier_seconds)
and
[`output_format_image_time_divisor_seconds`](/reference/settings/formats/output-format#output_format_image_time_divisor_seconds):
one unit of `t` is `output_format_image_time_multiplier_seconds / output_format_image_time_divisor_seconds`
seconds. With the default values (`1` and `60`) one unit of `t` is 1/60 of a second.

A frame is displayed until the next frame begins, so its duration is the difference between two consecutive
values of `t`. The last frame is displayed for as long as the frame before it. The animation loops forever.

```sql
SELECT
    number % 60 AS t,
    toInt32(intDiv(number, 60) % 64) AS x,
    toInt32((number * 7) % 64) AS y,
    toUInt8(255) AS v
FROM numbers(60 * 64)
INTO OUTFILE 'animation.png'
FORMAT PNG
SETTINGS output_format_image_width = 64, output_format_image_height = 64;
```

### Streaming the frames {#streaming-animation}

By default all frames are collected in memory and written out at the end of the query, which keeps one image
buffer per distinct value of `t` and lets `t` arrive in any order.

The setting
[`output_format_image_streaming_animation`](/reference/settings/formats/output-format#output_format_image_streaming_animation)
writes each frame out as soon as the next value of `t` is seen. Only one image buffer is kept in memory, and
frames reach the output while the query is still running, so a viewer can display them as they are produced.
In exchange:

- `t` must be non-decreasing; the query throws an exception otherwise. Add `ORDER BY t` if needed.
- The number of frames is not known when the header has to be written, so the `acTL` chunk declares an upper
  bound instead of the exact count. Browsers play such a file, but decoders that trust the declared count
  (for example, `Pillow` and some command-line `APNG` tools) report an error after the last real frame.
  An animation of a single frame is the exception: the whole result has been read by the time that frame is
  written, so the count is declared exactly and the output conforms to the specification.

Because an inline terminal image protocol carries the whole datastream as a single payload, the frames cannot
reach the terminal early and this setting only affects how much memory is used there. The exact frame count is
patched into the buffered payload before it is sent, so the caveat about the upper bound does not apply.

An animation is displayed only in the `iterm` terminal mode. The `sixel` protocol cannot represent an
animation at all, and the Kitty graphics protocol animates only through a separate flow of per-frame commands,
not through an animated datastream, so it would display just the first frame; both modes reject a result with
a `t` column.

## Displaying images in the terminal {#terminal-mode}

By default, the `PNG` format writes the raw image bytes. The setting
[`output_format_image_terminal_mode`](/reference/settings/formats/output-format#output_format_image_terminal_mode)
makes the format render the image directly to the terminal using an inline image protocol instead:

| Value           | Behaviour                                                                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|
| `` (empty)      | Write the raw image bytes (the default).                                                                |
| `iterm`         | Use the iTerm2 inline image protocol.                                                                   |
| `kitty`         | Use the Kitty graphics protocol. Cannot display an animation.                                           |
| `sixel`         | Use the Sixel protocol. The image is reduced to a fixed 6×6×6 palette and the alpha channel, if any, is composited over a black background. |
| `auto`          | If the output is a terminal, detect its capabilities and use `iterm`, `kitty`, or `sixel` (in this order); otherwise write the raw image bytes. |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

## Format settings {#format-settings}

| Setting                                        | Description                                                     | Default    |
|------------------------------------------------|-----------------------------------------------------------------|------------|
| `output_format_image_width`                    | Width of the output image in pixels.                            | `1024`     |
| `output_format_image_height`                   | Height of the output image in pixels.                           | `1024`     |
| `output_format_image_terminal_mode`            | Inline terminal image protocol (see above).                     | `` (empty) |
| `output_format_image_time_multiplier_seconds`  | Numerator of the time unit of the `t` column, in seconds.       | `1`        |
| `output_format_image_time_divisor_seconds`     | Denominator of the time unit of the `t` column, in seconds.     | `60`       |
| `output_format_image_streaming_animation`      | Write each frame as soon as `t` advances (see above).           | `0`        |
)DOCS_MD"});
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
