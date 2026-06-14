#include <Formats/PNGTerminalOutput.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// libpng-encoded payloads are sent to the terminal in base64; Kitty requires chunks of at most 4096 bytes.
    constexpr size_t KITTY_CHUNK_SIZE = 4096;

    /// Number of levels per color channel in the fixed Sixel palette (6 levels -> 6*6*6 = 216 colors).
    constexpr int SIXEL_LEVELS = 6;

    /// A run of this length or longer is worth encoding with the Sixel repeat introducer `!`.
    constexpr size_t SIXEL_MIN_RUN_TO_REPEAT = 4;

    bool envEquals(const char * name, const char * value)
    {
        const char * actual = std::getenv(name); // NOLINT(concurrency-mt-unsafe): terminal detection happens once at output time.
        return actual != nullptr && 0 == std::strcmp(actual, value);
    }

    bool envIsSet(const char * name)
    {
        return std::getenv(name) != nullptr; // NOLINT(concurrency-mt-unsafe): terminal detection happens once at output time.
    }

    bool detectITerm()
    {
        return envEquals("TERM_PROGRAM", "iTerm.app")
            || envEquals("TERM_PROGRAM", "WezTerm")
            || envEquals("LC_TERMINAL", "iTerm2");
    }

    bool detectKitty()
    {
        return envIsSet("KITTY_WINDOW_ID")
            || envEquals("TERM", "xterm-kitty")
            || envEquals("TERM_PROGRAM", "ghostty");
    }

    bool detectSixel()
    {
        /// There is no reliable environment-only way to detect Sixel support, so this is best-effort
        /// for a few terminals that are known to support it and advertise themselves via `TERM`.
        const char * term = std::getenv("TERM"); // NOLINT(concurrency-mt-unsafe): terminal detection happens once at output time.
        if (term == nullptr)
            return false;
        std::string_view t(term);
        return t.starts_with("foot") || t == "mlterm" || t.starts_with("yaft");
    }

    void writeSixelRun(WriteBuffer & out, char ch, size_t count)
    {
        if (count == 0)
            return;
        if (count >= SIXEL_MIN_RUN_TO_REPEAT)
        {
            writeChar('!', out);
            writeIntText(count, out);
            writeChar(ch, out);
        }
        else
        {
            for (size_t i = 0; i < count; ++i)
                writeChar(ch, out);
        }
    }
}

ImageTerminalMode parseImageTerminalMode(const String & mode, bool is_writing_to_terminal)
{
    if (mode.empty())
        return ImageTerminalMode::None;

    String lower;
    lower.reserve(mode.size());
    for (char c : mode)
        lower.push_back(toLowerIfAlphaASCII(c));

    if (lower == "iterm")
        return ImageTerminalMode::ITerm;
    if (lower == "kitty")
        return ImageTerminalMode::Kitty;
    if (lower == "sixel")
        return ImageTerminalMode::Sixel;
    if (lower == "auto")
    {
        if (!is_writing_to_terminal)
            return ImageTerminalMode::None;
        if (detectITerm())
            return ImageTerminalMode::ITerm;
        if (detectKitty())
            return ImageTerminalMode::Kitty;
        if (detectSixel())
            return ImageTerminalMode::Sixel;
        return ImageTerminalMode::None;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown value '{}' for setting 'output_format_image_terminal_mode'. "
        "Expected one of: '' (empty), 'iterm', 'kitty', 'sixel', 'auto'", mode);
}

void writeImageITerm(WriteBuffer & out, std::string_view png)
{
    /// ESC ] 1337 ; File = inline=1 ; size=<bytes> : <base64> BEL
    const std::string encoded = base64Encode(std::string(png));
    writeCString("\033]1337;File=inline=1;size=", out);
    writeIntText(png.size(), out);
    writeChar(':', out);
    out.write(encoded.data(), encoded.size());
    writeChar('\a', out);
    writeChar('\n', out);
}

void writeImageKitty(WriteBuffer & out, std::string_view png)
{
    /// ESC _ G <control data> ; <base64 chunk> ESC \, repeated for each chunk.
    /// The first chunk declares the PNG format (f=100) and the transmit-and-display action (a=T);
    /// `m=1` marks that more chunks follow, `m=0` marks the last one.
    const std::string encoded = base64Encode(std::string(png));
    const size_t total = encoded.size();
    size_t offset = 0;
    bool first = true;

    do
    {
        const size_t len = std::min(KITTY_CHUNK_SIZE, total - offset);
        const bool last = offset + len >= total;

        writeCString("\033_G", out);
        if (first)
            writeCString("f=100,a=T,", out);
        writeCString("m=", out);
        writeChar(last ? '0' : '1', out);
        writeChar(';', out);
        out.write(encoded.data() + offset, len);
        writeCString("\033\\", out);

        offset += len;
        first = false;
    } while (offset < total);

    writeChar('\n', out);
}

void writeImageSixel(WriteBuffer & out, const UInt8 * pixels, size_t width, size_t height, size_t channels)
{
    auto to_level = [](UInt8 c) -> int { return (c * (SIXEL_LEVELS - 1) + 127) / 255; };

    auto pixel_color = [&](size_t x, size_t y) -> int
    {
        const UInt8 * p = pixels + (y * width + x) * channels;
        UInt8 r = 0;
        UInt8 g = 0;
        UInt8 b = 0;
        if (channels == 1)
        {
            r = g = b = p[0];
        }
        else
        {
            r = p[0];
            g = p[1];
            b = p[2];
            if (channels == 4)
            {
                /// Composite over a black background using the alpha channel.
                const UInt32 a = p[3];
                r = static_cast<UInt8>(r * a / 255);
                g = static_cast<UInt8>(g * a / 255);
                b = static_cast<UInt8>(b * a / 255);
            }
        }
        return (to_level(r) * SIXEL_LEVELS + to_level(g)) * SIXEL_LEVELS + to_level(b);
    };

    /// Start of the Sixel data: DCS, then raster attributes (1:1 pixel aspect ratio and the image size).
    writeCString("\033Pq", out);
    writeCString("\"1;1;", out);
    writeIntText(width, out);
    writeChar(';', out);
    writeIntText(height, out);

    /// Define the full fixed palette up front (color number; RGB color space [2]; R, G, B in percent [0, 100]).
    for (int c = 0; c < SIXEL_LEVELS * SIXEL_LEVELS * SIXEL_LEVELS; ++c)
    {
        const int r = c / (SIXEL_LEVELS * SIXEL_LEVELS);
        const int g = (c / SIXEL_LEVELS) % SIXEL_LEVELS;
        const int b = c % SIXEL_LEVELS;
        writeChar('#', out);
        writeIntText(c, out);
        writeCString(";2;", out);
        writeIntText(r * 100 / (SIXEL_LEVELS - 1), out);
        writeChar(';', out);
        writeIntText(g * 100 / (SIXEL_LEVELS - 1), out);
        writeChar(';', out);
        writeIntText(b * 100 / (SIXEL_LEVELS - 1), out);
    }

    /// Sixels encode 6 vertical pixels at a time, so the image is processed in horizontal bands of 6 rows.
    std::vector<int> band_colors(width * 6);
    std::array<bool, SIXEL_LEVELS * SIXEL_LEVELS * SIXEL_LEVELS> present{};
    std::string line;

    for (size_t band_top = 0; band_top < height; band_top += 6)
    {
        const size_t rows_in_band = std::min<size_t>(6, height - band_top);

        present.fill(false);
        for (size_t sub = 0; sub < rows_in_band; ++sub)
            for (size_t x = 0; x < width; ++x)
            {
                const int c = pixel_color(x, band_top + sub);
                band_colors[sub * width + x] = c;
                present[c] = true;
            }

        bool first_color = true;
        for (size_t c = 0; c < present.size(); ++c)
        {
            if (!present[c])
                continue;

            /// Overlay the next color onto the same band by returning to the start of the line.
            if (!first_color)
                writeChar('$', out);
            first_color = false;

            writeChar('#', out);
            writeIntText(c, out);

            line.assign(width, '?');
            for (size_t x = 0; x < width; ++x)
            {
                int bits = 0;
                for (size_t sub = 0; sub < rows_in_band; ++sub)
                    if (band_colors[sub * width + x] == static_cast<int>(c))
                        bits |= (1 << sub);
                line[x] = static_cast<char>(0x3F + bits);
            }

            /// Trailing background sixels can be omitted.
            const size_t last = line.find_last_not_of('?');
            line.resize(last + 1);

            size_t run_len = 0;
            char run_char = 0;
            for (char ch : line)
            {
                if (run_len != 0 && ch == run_char)
                {
                    ++run_len;
                }
                else
                {
                    writeSixelRun(out, run_char, run_len);
                    run_char = ch;
                    run_len = 1;
                }
            }
            writeSixelRun(out, run_char, run_len);
        }

        /// Move down to the next band (graphics new line), except after the last one.
        if (band_top + 6 < height)
            writeChar('-', out);
    }

    /// String Terminator ends the Sixel data.
    writeCString("\033\\", out);
    writeChar('\n', out);
}

}
