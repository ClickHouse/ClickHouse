#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// A grid of character cells for drawing text pictures with box-drawing characters, such as `EXPLAIN PLAN tree = 1`.
/// Every cell holds one character: a code point and the zero-width code points that follow it (combining marks, a
/// virama, joiners). A character of display width 2 (e.g. CJK) takes two cells; the second one is a continuation that is
/// not printed. The grid grows as text is put into it.
class TextCanvas
{
public:
    /// Writes `text` starting at column `x` of line `y`, one character per cell (two cells for wide characters),
    /// overwriting what was there. A zero-width code point is kept with the character before it.
    void put(size_t x, size_t y, std::string_view text);

    /// Draws a box with the light box-drawing characters, `width` columns wide and `boxHeight(text_lines.size())` lines
    /// tall, with one line of `text_lines` per inner line after one space of padding.
    void box(size_t x, size_t y, size_t width, const std::vector<String> & text_lines);

    /// The number of lines of a box with `text_lines` lines of text: the text and the top and bottom borders.
    static size_t boxHeight(size_t text_lines) { return text_lines + 2; }

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

    /// `text` split into lines of at most `max_width` display columns. A line is broken after the last space that
    /// fits, and a word wider than a line is split between two code points. The spaces at a break are dropped. A
    /// zero-width code point stays with the character before it. Control characters are replaced first (see
    /// `replaceControlCharacters`). An empty text gives one empty line.
    static std::vector<String> wrapToWidth(std::string_view text, size_t max_width);

    /// `text` with every ASCII control character (a tab, a newline, ESC, ...) replaced by `�`. A control character has no
    /// width of its own, or one that depends on its position, so it would break the measured width of a box.
    static String replaceControlCharacters(std::string_view text);

private:
    /// Cell contents; an empty string is the continuation of a wide character.
    std::vector<std::vector<String>> lines;

    String & cell(size_t x, size_t y);
};

}
