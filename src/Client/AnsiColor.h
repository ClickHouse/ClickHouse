#pragma once

#include <cstdint>
#include <string>

namespace DB::AnsiColor
{

/// Packed color/attribute value.
///
///   bits  0..7 : foreground palette index (0..255, 256-color cube via rgb666)
///   bit       8 : foreground is set
///   bits  9..16: background palette index
///   bit      17: background is set
///   bit      18: bold
///   bit      19: underline
///
/// `Color::DEFAULT` = 0 means "no override" (terminal default).
using Color = std::uint32_t;

inline constexpr std::uint32_t FG_SET    = 1u << 8;
inline constexpr std::uint32_t BG_SHIFT  = 9u;
inline constexpr std::uint32_t BG_SET    = 1u << 17;
inline constexpr std::uint32_t BOLD      = 1u << 18;
inline constexpr std::uint32_t UNDERLINE = 1u << 19;

inline constexpr Color basic(std::uint8_t idx) { return static_cast<Color>(idx) | FG_SET; }

namespace c
{
    inline constexpr Color DEFAULT       = 0;
    inline constexpr Color BLACK         = basic(0);
    inline constexpr Color RED           = basic(1);
    inline constexpr Color GREEN         = basic(2);
    inline constexpr Color BROWN         = basic(3);
    inline constexpr Color BLUE          = basic(4);
    inline constexpr Color MAGENTA       = basic(5);
    inline constexpr Color CYAN          = basic(6);
    inline constexpr Color LIGHTGRAY     = basic(7);
    inline constexpr Color GRAY          = basic(8);
    inline constexpr Color BRIGHTRED     = basic(9);
    inline constexpr Color BRIGHTGREEN   = basic(10);
    inline constexpr Color BRIGHTYELLOW  = basic(11);
    inline constexpr Color BRIGHTBLUE    = basic(12);
    inline constexpr Color BRIGHTMAGENTA = basic(13);
    inline constexpr Color BRIGHTCYAN    = basic(14);
    inline constexpr Color WHITE         = basic(15);
}

/// 6x6x6 cube color (each component 0..5).
inline constexpr Color rgb666(int r, int g, int b)
{
    auto idx = static_cast<std::uint8_t>(16 + 36 * r + 6 * g + b);
    return basic(idx);
}

inline constexpr Color bold(Color c)      { return c | BOLD; }
inline constexpr Color underline(Color c) { return c | UNDERLINE; }
inline constexpr Color bg(Color fg_color, Color bg_color)
{
    /// Take the fg palette index of bg_color and place it in the bg slot.
    if (!(bg_color & FG_SET))
        return fg_color;
    std::uint32_t idx = bg_color & 0xFFu;
    return fg_color | (idx << BG_SHIFT) | BG_SET;
}

/// Build the ANSI SGR escape sequence for the given color.
/// Returns "" if `c == DEFAULT` (caller may want to emit reset separately).
std::string ansiSequence(Color c);

}
