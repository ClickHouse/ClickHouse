#include <Processors/QueryPlan/Fragments/TextCanvas.h>

#include <IO/WriteBuffer.h>
#include <Common/UTF8Helpers.h>

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

String & TextCanvas::cell(size_t x, size_t y)
{
    if (lines.size() <= y)
        lines.resize(y + 1);
    auto & line = lines[y];
    if (line.size() <= x)
        line.resize(x + 1, " ");
    return line[x];
}

void TextCanvas::put(size_t x, size_t y, std::string_view text)
{
    forEachCodePoint(text, [&](std::string_view code_point, size_t width)
    {
        if (width == 0)
            return;
        cell(x, y) = String(code_point);
        for (size_t i = 1; i < width; ++i)
            cell(x + i, y).clear();
        x += width;
    });
}

void TextCanvas::box(size_t x, size_t y, size_t width, std::string_view text)
{
    String horizontal;
    for (size_t i = 2; i < width; ++i)
        horizontal += "─";

    put(x, y, "┌" + horizontal + "┐");
    put(x, y + 1, "│ ");
    put(x + 2, y + 1, text);
    put(x + width - 1, y + 1, "│");
    put(x, y + 2, "└" + horizontal + "┘");
}

void TextCanvas::frame(size_t x, size_t y, size_t width, size_t height, std::string_view title)
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

std::string_view TextCanvas::at(size_t x, size_t y) const
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

size_t TextCanvas::displayWidth(std::string_view text)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(text.data()), text.size());
}

String TextCanvas::replaceControlCharacters(std::string_view text)
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

String TextCanvas::cutToWidth(std::string_view raw_text, size_t max_width)
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
