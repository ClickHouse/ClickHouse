#include <Columns/IColumn.h>
#include <Formats/PrettyFormatHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Common/formatReadable.h>
#include <Common/UTF8Helpers.h>


static constexpr const char * GRAY_COLOR = "\033[90m";
static constexpr const char * RED_COLOR = "\033[31m";
static constexpr const char * UNDERSCORE = "\033[4m";
static constexpr const char * RESET_COLOR = "\033[0m";


namespace DB
{

void writeReadableNumberTipIfSingleValue(WriteBuffer & out, const Chunk & chunk, const FormatSettings & settings, bool color)
{
    if (chunk.getNumRows() == 1 && chunk.getNumColumns() == 1)
        writeReadableNumberTip(out, *chunk.getColumns()[0], 0, settings, color);
}

void writeReadableNumberTip(WriteBuffer & out, const IColumn & column, size_t row, const FormatSettings & settings, bool color)
{
    if (column.isNullAt(row))
        return;

    auto value = column.getFloat64(row);
    auto abs_value = abs(value);
    auto threshold = settings.pretty.single_large_number_tip_threshold;

    if (threshold && isFinite(value) && abs_value > threshold
        /// Most (~99.5%) of 64-bit hash values are in this range, and it is not necessarily to highlight them:
        && !(abs_value > 1e17 && abs_value < 1.844675e19))
    {
        if (color)
            writeCString(GRAY_COLOR, out);
        writeCString(" -- ", out);
        formatReadableQuantity(value, out, 2);
        if (color)
            writeCString(RESET_COLOR, out);
    }
}


String highlightDigitGroups(String source)
{
    if (source.size() <= 4)
        return source;

    bool is_regular_number = true;
    size_t num_digits_before_decimal = 0;
    for (auto c : source)
    {
        if (c == '-' || c == ' ')
            continue;
        if (c == '.')
            break;
        if (c >= '0' && c <= '9')
        {
            ++num_digits_before_decimal;
        }
        else
        {
            is_regular_number = false;
            break;
        }
    }

    if (!is_regular_number || num_digits_before_decimal <= 4)
        return source;

    String result;
    size_t size = source.size();
    result.reserve(2 * size);

    bool before_decimal = true;
    size_t digit_num = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto c = source[i];
        if (before_decimal && c >= '0' && c <= '9')
        {
            ++digit_num;
            size_t offset = num_digits_before_decimal - digit_num;
            if (offset && offset % 3 == 0)
            {
                result += UNDERSCORE;
                result += c;
                result += RESET_COLOR;
            }
            else
            {
                result += c;
            }
        }
        else if (c == '.')
        {
            before_decimal = false;
            result += c;
        }
        else
        {
            result += c;
        }
    }

    return result;
}


String highlightTrailingSpaces(String source)
{
    if (source.empty())
        return source;

    const char * last_significant = find_last_not_symbols_or_null<' ', '\t', '\n', '\r', '\f', '\v'>(source.data(), source.data() + source.size());
    size_t highlight_start_pos = 0;
    if (last_significant)
    {
        highlight_start_pos = last_significant + 1 - source.data();
        if (highlight_start_pos >= source.size())
            return source;
    }

    return source.substr(0, highlight_start_pos) + RED_COLOR + UNDERSCORE + source.substr(highlight_start_pos, std::string::npos) + RESET_COLOR;
}


std::pair<String, size_t> truncateName(String name, size_t cut_to, size_t hysteresis, bool ascii)
{
    size_t length = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(name.data()), name.size());

    if (!cut_to || length <= cut_to + hysteresis || isValidIdentifier(name))
        return {name, length};

    /// We cut characters in the middle and insert filler there.
    const char * filler = ascii ? "~" : "⋯";

    size_t prefix_chars = cut_to / 2;
    size_t suffix_chars = (cut_to - 1) / 2;
    size_t suffix_chars_begin = length - suffix_chars;

    size_t prefix_bytes = UTF8::computeBytesBeforeWidth(reinterpret_cast<const UInt8 *>(name.data()), name.size(), 0, prefix_chars);
    size_t suffix_bytes_begin = UTF8::computeBytesBeforeWidth(reinterpret_cast<const UInt8 *>(name.data()), name.size(), 0, suffix_chars_begin);

    name = name.substr(0, prefix_bytes) + filler + name.substr(suffix_bytes_begin, std::string::npos);

    return {name, cut_to};
}

}
