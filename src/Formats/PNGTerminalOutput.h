#pragma once

#include <string_view>
#include <base/types.h>

namespace DB
{

class WriteBuffer;

/// How to deliver an image output format (such as `PNG`) to the output.
enum class ImageTerminalMode : uint8_t
{
    None,   /// Write the raw image bytes.
    ITerm,  /// iTerm2 inline image protocol.
    Kitty,  /// Kitty graphics protocol.
    Sixel,  /// Sixel protocol.
};

/// Parse the `output_format_image_terminal_mode` setting.
///
/// For `auto`, the terminal capabilities are detected (only when `is_writing_to_terminal`), preferring
/// `iterm`, then `kitty`, then `sixel`; if none is detected, falls back to writing the raw image bytes.
/// Throws on an invalid value.
ImageTerminalMode parseImageTerminalMode(const String & mode, bool is_writing_to_terminal);

/// Write an already encoded PNG image to `out` using the iTerm2 inline image protocol.
void writeImageITerm(WriteBuffer & out, std::string_view png);

/// Write an already encoded PNG image to `out` using the Kitty graphics protocol.
void writeImageKitty(WriteBuffer & out, std::string_view png);

/// Encode a tightly packed `width` x `height` image with `channels` (1, 3, or 4) bytes per pixel into `out`
/// using the Sixel protocol. The image is reduced to a fixed 6x6x6 color palette; the alpha channel, if any,
/// is composited over a black background.
void writeImageSixel(WriteBuffer & out, const UInt8 * pixels, size_t width, size_t height, size_t channels);

}
