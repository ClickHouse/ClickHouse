#include <Columns/IColumn.h>
#include <Formats/PrettyFormatHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Common/formatReadable.h>
#include <Common/UTF8Helpers.h>

#include <array>


static constexpr const char * GRAY_COLOR = "\033[90m";
static constexpr const char * RED_COLOR = "\033[31m";
static constexpr const char * UNDERSCORE = "\033[4m";
static constexpr const char * RESET_COLOR = "\033[0m";


namespace DB
{

void writeReadableNumberTip(WriteBuffer & out, const IColumn & column, size_t row, const FormatSettings & settings, bool color, size_t max_width)
{
    if (column.isNullAt(row))
        return;

    auto value = column.getFloat64(row);
    auto abs_value = abs(value);
    auto threshold = settings.pretty.single_large_number_tip_threshold;

    if (threshold && isFinite(value) && abs_value > static_cast<double>(threshold)
        /// Most (~99.5%) of 64-bit hash values are in this range, and it is not necessarily to highlight them:
        && !(abs_value > 1e17 && abs_value < 1.844675e19))
    {
        std::string output = formatReadableQuantity(value, 2);
        size_t tip_width = output.size() + 4;

        if (tip_width <= max_width)
        {
            if (color)
                writeCString(GRAY_COLOR, out);
            writeCString(" -- ", out);
            writeString(output, out);
            if (color)
                writeCString(RESET_COLOR, out);
        }
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


namespace
{

/// Non-printable C0 control characters (0x00..0x1F) and DEL (0x7F). Unlike C-style escape sequences
/// (\0, \t, ...), the Unicode "Control Pictures" of these bytes cannot be confused with the literal
/// characters they represent, so the "pretty" Vertical format displays them to make the bytes
/// visible instead of letting the terminal silently swallow them.
/// See https://en.wikipedia.org/wiki/Control_Pictures
bool isControlCharacter(unsigned char byte)
{
    return byte < 0x20 || byte == 0x7F;
}

/// UTF-8 encoding of the Unicode "Control Picture" for a control byte: the code point is
/// U+2400 + byte for 0x00..0x1F, and U+2421 for DEL (0x7F). All of these lie in U+2400..U+2421,
/// whose UTF-8 encoding is 0xE2 0x90 (0x80 + low 6 bits).
std::array<char, 3> controlCharacterPicture(unsigned char byte)
{
    const unsigned char code = (byte == 0x7F) ? 0x21 : byte;
    return {static_cast<char>(0xE2), static_cast<char>(0x90), static_cast<char>(0x80 + code)};
}

}


String replaceControlCharactersWithPictures(String source)
{
    bool has_control_characters = false;
    for (char c : source)
    {
        if (isControlCharacter(static_cast<unsigned char>(c)))
        {
            has_control_characters = true;
            break;
        }
    }

    if (!has_control_characters)
        return source;

    String result;
    /// Every replaced byte expands to a 3-byte UTF-8 sequence.
    result.reserve(source.size() + source.size() / 2);

    for (char c : source)
    {
        const auto byte = static_cast<unsigned char>(c);
        if (isControlCharacter(byte))
        {
            const auto picture = controlCharacterPicture(byte);
            result.append(picture.data(), picture.size());
        }
        else
        {
            result += c;
        }
    }

    return result;
}


WriteBufferReplacingControlCharacters::WriteBufferReplacingControlCharacters(WriteBuffer & out_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size)
    , out(out_)
{
}

WriteBufferReplacingControlCharacters::~WriteBufferReplacingControlCharacters()
{
    if (!finalized && !canceled)
        cancel();
}

void WriteBufferReplacingControlCharacters::nextImpl()
{
    if (!offset())
        return;

    const char * const begin = working_buffer.begin();
    const char * const end = begin + offset();

    /// Forward runs of ordinary bytes in bulk, expanding each control byte to its Control Picture.
    const char * run_begin = begin;
    for (const char * pos = begin; pos != end; ++pos)
    {
        const auto byte = static_cast<unsigned char>(*pos);
        if (isControlCharacter(byte))
        {
            if (pos != run_begin)
                out.write(run_begin, pos - run_begin);

            const auto picture = controlCharacterPicture(byte);
            out.write(picture.data(), picture.size());

            run_begin = pos + 1;
        }
    }

    if (end != run_begin)
        out.write(run_begin, end - run_begin);
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
