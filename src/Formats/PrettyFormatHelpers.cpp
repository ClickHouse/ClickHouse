#include <Formats/PrettyFormatHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Common/formatReadable.h>


static constexpr const char * GRAY_COLOR = "\033[90m";
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
    auto threshold = settings.pretty.output_format_pretty_single_large_number_tip_threshold;

    if (threshold && isFinite(value) && abs(value) > threshold)
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

}
