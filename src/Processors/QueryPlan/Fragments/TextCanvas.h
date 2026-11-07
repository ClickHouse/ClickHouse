#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

class WriteBuffer;

/// A grid of character cells for drawing text pictures with box-drawing characters, such as `EXPLAIN PLAN tree = 1`.
///
/// The model: `lines[y][x]` is one cell, and one cell is one terminal column. Coordinates are `(x, y)` = (column,
/// line), starting at 0.
/// - A cell holds the bytes of one visible character, as a `String` rather than a `char`, because a character can be
///   several UTF-8 bytes (`─`, `é`, `键`).
/// - Zero-width code points (combining marks, a virama, joiners) go into the cell of the character before them:
///   `e` followed by U+0301 is one cell `é`, which keeps identifiers such as `café` and `क्ष` intact.
/// - A character of display width 2 (e.g. CJK) takes two cells: the first holds the character, the second is an empty
///   continuation that `writeTo` skips, so every following column stays in place.
/// - The grid grows as text is put into it: writing beyond the current size adds lines and pads them with spaces.
/// - A later write overwrites a cell, so the order of the calls matters: `TreeBuilder::drawFrom` draws the frames, then
///   the boxes, then the edges, whose joints (`┴`, `▲`, `┼`) replace single characters of the borders.
///
/// The canvas knows nothing about plans, fragments or exchanges: `TreeBuilder` in `FragmentGraphTree.cpp` decides every
/// coordinate. Example:
///
///     TextCanvas canvas;
///     canvas.frame(0, 0, 30, 7, "F0 · stage_0");
///     auto lines = TextCanvas::wrapToWidth("Aggregating (partial)", 40);  /// {"Aggregating (partial)"}
///     size_t width = TextCanvas::displayWidth(lines[0]) + 4;             /// 25
///     canvas.box(2, 2, width, lines);                                    /// lines 2 to 4
///     canvas.put(2 + width / 2, 2, "┴");                                 /// the edge to the step that reads this box
///     canvas.writeTo(buf);
///
///     ┌─ F0 · stage_0 ─────────────┐
///     │                            │
///     │ ┌───────────┴───────────┐  │
///     │ │ Aggregating (partial) │  │
///     │ └───────────────────────┘  │
///     │                            │
///     └────────────────────────────┘
class TextCanvas
{
public:
    /// Writes `text` starting at column `x` of line `y`, one character per cell (two cells for wide characters),
    /// overwriting what was there. A zero-width code point is kept with the character before it. The other drawing
    /// functions draw through `put`.
    void put(size_t x, size_t y, std::string_view text);

    /// Draws a box with its top-left corner at `(x, y)`, `width` columns wide and
    /// `boxHeight(text_lines.size(), details.size())` lines tall: `┌───┐`, then one `│ line │` per line of `text_lines`
    /// with one space of padding on each side; with `details`, a separator `├───┤` and one line per detail under it;
    /// then `└───┘`. The caller computes `width`, usually the widest line plus 4.
    void box(size_t x, size_t y, size_t width, const std::vector<String> & text_lines, const std::vector<String> & details = {});

    /// The number of lines of a box with `text_lines` lines of text and `details_count` detail lines: the text, the top
    /// and bottom borders and, with details, the separator and the details.
    static size_t boxHeight(size_t text_lines, size_t details_count = 0)
    {
        return text_lines + 2 + (details_count == 0 ? 0 : details_count + 1);
    }

    /// Draws a frame `width` columns wide and `height` lines tall, with `title` in the top border: `┌─ title ───┐`.
    void frame(size_t x, size_t y, size_t width, size_t height, std::string_view title);

    /// The content of a cell, or an empty view outside the grid.
    [[nodiscard]] std::string_view at(size_t x, size_t y) const;

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

    /// `text` with every ASCII control character (a tab, a newline, ESC, ...) replaced by `�`. A control character has
    /// no width of its own, or one that depends on the terminal or its position, so it would break the measured width
    /// of a box.
    static String replaceControlCharacters(std::string_view text);

private:
    /// Cell contents; an empty string is the continuation of a wide character.
    std::vector<std::vector<String>> lines;

    String & cell(size_t x, size_t y);
};

}
