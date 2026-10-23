#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// A grid of character cells for drawing text pictures with box-drawing characters, such as `EXPLAIN PLAN tree = 1`.
/// Every cell holds one code point. A character of display width 2 (e.g. CJK) takes two cells; the second one is a
/// continuation that is not printed. The grid grows as text is put into it.
class TextCanvas
{
public:
    /// Writes `text` starting at column `x` of line `y`, one code point per cell (two cells for wide characters),
    /// overwriting what was there.
    void put(size_t x, size_t y, std::string_view text);

    /// Draws a box with the light box-drawing characters, `width` columns wide and 3 lines tall, with `text`
    /// on the middle line after one space of padding.
    void box(size_t x, size_t y, size_t width, std::string_view text);

    /// Draws a frame `width` columns wide and `height` lines tall, with `title` in the top border: `┌─ title ───┐`.
    void frame(size_t x, size_t y, size_t width, size_t height, std::string_view title);

    /// The content of a cell, or an empty view outside the grid.
    std::string_view at(size_t x, size_t y) const;

    /// Writes all lines, each without trailing spaces and followed by a newline.
    void writeTo(WriteBuffer & out) const;

    /// The display width of `text`.
    static size_t displayWidth(std::string_view text);

    /// `text` cut to at most `max_width` display columns; a cut text ends with `…`. Control characters are replaced
    /// first (see `replaceControlCharacters`).
    static String cutToWidth(std::string_view text, size_t max_width);

    /// `text` with every ASCII control character (a tab, a newline, ESC, ...) replaced by `�`. A control character has no
    /// width of its own, or one that depends on its position, so it would break the measured width of a box.
    static String replaceControlCharacters(std::string_view text);

private:
    /// Cell contents; an empty string is the continuation of a wide character.
    std::vector<std::vector<String>> lines;

    String & cell(size_t x, size_t y);
};

}
