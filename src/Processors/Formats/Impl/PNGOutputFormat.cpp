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
    factory.setDocumentation(FORMAT_NAME, Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |   ✗   |

## Description {#description}

Renders the result of a query as a PNG image. This is useful as a built-in visualization tool.

The size of the output image is fixed by the settings
[`output_format_image_width`](/reference/settings/formats#output_format_image_width) and
[`output_format_image_height`](/reference/settings/formats#output_format_image_height)
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

## Displaying images in the terminal {#terminal-mode}

By default, the `PNG` format writes the raw image bytes. The setting
[`output_format_image_terminal_mode`](/reference/settings/formats#output_format_image_terminal_mode)
makes the format render the image directly to the terminal using an inline image protocol instead:

| Value           | Behaviour                                                                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|
| `` (empty)      | Write the raw image bytes (the default).                                                                |
| `iterm`         | Use the iTerm2 inline image protocol.                                                                   |
| `kitty`         | Use the Kitty graphics protocol.                                                                        |
| `sixel`         | Use the Sixel protocol. The image is reduced to a fixed 6×6×6 palette and the alpha channel, if any, is composited over a black background. |
| `auto`          | If the output is a terminal, detect its capabilities and use `iterm`, `kitty`, or `sixel` (in this order); otherwise write the raw image bytes. |

```sql
SELECT toUInt8(x * 25) AS r, toUInt8(y * 25) AS g, toUInt8((x + y) * 12) AS b
FROM (SELECT number % 10 AS x, intDiv(number, 10) AS y FROM numbers(100))
FORMAT PNG
SETTINGS output_format_image_width = 10, output_format_image_height = 10, output_format_image_terminal_mode = 'auto';
```

## Format settings {#format-settings}

| Setting                              | Description                                  | Default    |
|--------------------------------------|----------------------------------------------|------------|
| `output_format_image_width`          | Width of the output image in pixels.         | `1024`     |
| `output_format_image_height`         | Height of the output image in pixels.        | `1024`     |
| `output_format_image_terminal_mode`  | Inline terminal image protocol (see above).  | `` (empty) |
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
