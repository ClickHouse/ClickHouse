#include <string>
#include <base/terminalColors.h>


std::string setColor(UInt64 hash)
{
    /// Make a random RGB color that has constant brightness.
    /// https://en.wikipedia.org/wiki/YCbCr

    /// Note that this is darker than the middle relative luminance, see "Gamma_correction" and "Luma_(video)".
    /// It still looks awesome.
    UInt8 y = 128;

    UInt8 cb = hash % 256;
    UInt8 cr = hash / 256 % 256;

    UInt8 r = std::max(0.0, std::min(255.0, y + 1.402 * (cr - 128)));
    UInt8 g = std::max(0.0, std::min(255.0, y - 0.344136 * (cb - 128) - 0.714136 * (cr - 128)));
    UInt8 b = std::max(0.0, std::min(255.0, y + 1.772 * (cb - 128)));

    /// ANSI escape sequence to set 24-bit foreground font color in terminal.
    return "\033[38;2;" + std::to_string(r) + ";" + std::to_string(g) + ";" + std::to_string(b) + "m";
}

const char * setColorForLogPriority(int priority)
{
    if (priority < 1 || priority > 8)
        return "";

    static const char * colors[] =
    {
        "",
        "\033[1;41m",   /// Fatal
        "\033[7;31m",   /// Critical
        "\033[1;31m",   /// Error
        "\033[0;31m",   /// Warning
        "\033[0;33m",   /// Notice
        "\033[1m",      /// Information
        "",             /// Debug
        "\033[2m",      /// Trace
    };

    return colors[priority];
}

const char * resetColor()
{
    return "\033[0m";
}
