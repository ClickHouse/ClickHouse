#include <Processors/QueryPlan/Fragments/TextCanvas.h>

#include <IO/WriteBuffer.h>
#include <Common/UTF8Helpers.h>

#include <optional>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace
{

/// Splits `text` into code points, each with its display width.
template <typename Callback>
void forEachCodePoint(std::string_view text, Callback && callback)
{
    size_t pos = 0;
    while (pos < text.size())
    {
        size_t length = std::min(UTF8::seqLength(static_cast<UInt8>(text[pos])), text.size() - pos);
        std::string_view code_point = text.substr(pos, length);
        callback(code_point, UTF8::computeWidth(reinterpret_cast<const UInt8 *>(code_point.data()), code_point.size()));
        pos += length;
    }
}

}

String & TextCanvas::cell(const size_t x, const size_t y)
{
    if (lines.size() <= y)
        lines.resize(y + 1);
    auto & line = lines[y];
    if (line.size() <= x)
        line.resize(x + 1, " ");
    return line[x];
}

void TextCanvas::put(size_t x, const size_t y, const std::string_view text)
{
    /// The cell of the last character put: a zero-width code point (a combining mark, a virama, a joiner) belongs to it
    /// and is appended to it, taking no column of its own. A text that starts with one attaches it to the cell on its left.
    std::optional<size_t> last_x;
    if (x > 0)
        last_x = x - 1;
    forEachCodePoint(text, [&](std::string_view code_point, size_t width)
    {
        if (width == 0)
        {
            if (last_x)
                cell(*last_x, y) += code_point;
            return;
        }
        cell(x, y) = String(code_point);
        for (size_t i = 1; i < width; ++i)
            cell(x + i, y).clear();
        last_x = x;
        x += width;
    });
}

void TextCanvas::box(
    const size_t x, const size_t y, const size_t width, const VectorWithMemoryTracking<String> & text_lines, const VectorWithMemoryTracking<String> & details)
{
    String horizontal;
    for (size_t i = 2; i < width; ++i)
        horizontal += "─";

    const auto put_line = [&](size_t line_y, std::string_view text)
    {
        put(x, line_y, "│ ");
        put(x + 2, line_y, text);
        put(x + width - 1, line_y, "│");
    };

    put(x, y, "┌" + horizontal + "┐");
    size_t line_y = y + 1;
    for (const auto & line : text_lines)
        put_line(line_y++, line);
    if (!details.empty())
    {
        put(x, line_y, "├" + horizontal + "┤");
        ++line_y;
        for (const auto & detail : details)
            put_line(line_y++, detail);
    }
    put(x, line_y, "└" + horizontal + "┘");
}

void TextCanvas::frame(const size_t x, const size_t y, const size_t width, const size_t height, const std::string_view title)
{
    String bottom;
    for (size_t i = 2; i < width; ++i)
        bottom += "─";

    /// `┌─ title ` and then the border up to the corner.
    put(x, y, "┌" + bottom + "┐");
    put(x + 1, y, "─ ");
    put(x + 3, y, title);
    put(x + 3 + displayWidth(title), y, " ");
    for (size_t i = 1; i + 1 < height; ++i)
    {
        put(x, y + i, "│");
        put(x + width - 1, y + i, "│");
    }
    put(x, y + height - 1, "└" + bottom + "┘");
}

std::string_view TextCanvas::at(const size_t x, const size_t y) const
{
    if (y >= lines.size() || x >= lines[y].size())
        return {};
    return lines[y][x];
}

void TextCanvas::writeTo(WriteBuffer & out) const
{
    for (const auto & line : lines)
    {
        size_t end = line.size();
        while (end > 0 && line[end - 1] == " ")
            --end;
        for (size_t i = 0; i < end; ++i)
            out.write(line[i].data(), line[i].size());
        out.write('\n');
    }
}

size_t TextCanvas::displayWidth(const std::string_view text)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(text.data()), text.size());
}

String TextCanvas::replaceControlCharacters(const std::string_view text)
{
    String result;
    result.reserve(text.size());
    for (char c : text)
    {
        if (static_cast<UInt8>(c) < 0x20 || c == 0x7F)
            result += "�";
        else
            result += c;
    }
    return result;
}

VectorWithMemoryTracking<String> TextCanvas::wrapToWidth(const std::string_view raw_text, const size_t max_width)
{
    const String clean_text = replaceControlCharacters(raw_text);

    VectorWithMemoryTracking<String> result;
    String line;
    size_t width = 0;
    /// The position in `line` after its last space, and the width of the text after it: where the line is broken.
    std::optional<size_t> break_position;
    size_t width_after_break = 0;

    const auto push_line = [&](String text)
    {
        while (!text.empty() && text.back() == ' ')
            text.pop_back();
        result.push_back(std::move(text));
    };

    forEachCodePoint(clean_text, [&](std::string_view code_point, size_t code_point_width)
    {
        while (code_point_width > 0 && width + code_point_width > max_width && !line.empty())
        {
            if (break_position)
            {
                push_line(line.substr(0, *break_position));
                line = line.substr(*break_position);
                width = width_after_break;
                break_position.reset();
            }
            else
            {
                push_line(std::move(line));
                line.clear();
                width = 0;
            }
        }

        /// A continued line does not start with the spaces of the break.
        if (code_point == " " && line.empty() && !result.empty())
            return;

        line += code_point;
        width += code_point_width;
        if (code_point == " ")
        {
            break_position = line.size();
            width_after_break = 0;
        }
        else if (break_position)
            width_after_break += code_point_width;
    });

    if (!line.empty() || result.empty())
        push_line(std::move(line));
    return result;
}

String TextCanvas::cutToWidth(const std::string_view raw_text, const size_t max_width)
{
    const String clean_text = replaceControlCharacters(raw_text);
    std::string_view text = clean_text;
    if (displayWidth(text) <= max_width)
        return String(text);

    /// Keep code points while they and the `…` fit.
    String result;
    size_t width = 0;
    bool full = false;
    forEachCodePoint(text, [&](std::string_view code_point, size_t code_point_width)
    {
        if (full || width + code_point_width + 1 > max_width)
        {
            full = true;
            return;
        }
        result += code_point;
        width += code_point_width;
    });
    return result + "…";
}

}
