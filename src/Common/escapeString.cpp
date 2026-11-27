#include <Common/escapeString.h>

#include <Common/Hex.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <widechar_width.h>

#include <algorithm>

namespace DB
{

String escapeString(std::string_view value)
{
    WriteBufferFromOwnString buf;
    writeEscapedString(value, buf);
    return buf.str();
}

namespace
{

/// Unicode spaces and Hangul fillers: they have a positive width in a terminal, but nothing is drawn.
bool isBlankCharacter(UInt32 code_point)
{
    switch (code_point)
    {
        /// Unicode space separators, except the ASCII space and U+1680 OGHAM SPACE MARK, which has a visible glyph.
        case 0x00A0: /// NO-BREAK SPACE
        case 0x202F: /// NARROW NO-BREAK SPACE
        case 0x205F: /// MEDIUM MATHEMATICAL SPACE
        case 0x3000: /// IDEOGRAPHIC SPACE
        /// Hangul fillers. U+1160 HANGUL JUNGSEONG FILLER is not listed, because it is a combining character.
        case 0x115F: /// HANGUL CHOSEONG FILLER
        case 0x3164: /// HANGUL FILLER
        case 0xFFA0: /// HALFWIDTH HANGUL FILLER
            return true;
        default:
            /// EN QUAD ... HAIR SPACE
            return code_point >= 0x2000 && code_point <= 0x200A;
    }
}

}

String escapeNonPrintableCharacters(String value)
{
    String res;
    size_t copied = 0;
    for (size_t pos = 0; pos < value.size();)
    {
        if (isPrintableASCII(value[pos]))
        {
            /// Fast path, the character is printable ASCII, so we can skip it without escaping.
            ++pos;
            continue;
        }

        size_t length = std::min(UTF8::seqLength(static_cast<UInt8>(value[pos])), value.size() - pos);
        const auto code_point = UTF8::convertUTF8ToCodePoint(&value[pos], length);
        /// The other negative values are for characters that are not shown, or are shown as a part of the previous one.
        const int width = code_point ? widechar_wcwidth(*code_point) : widechar_nonprint;
        if ((width > 0 || width == widechar_ambiguous || width == widechar_widened_in_9) && !isBlankCharacter(*code_point))
        {
            pos += length;
            continue;
        }

        /// One byte at a time: the rest of an invisible character is escaped as bytes of invalid UTF-8.
        res.append(value, copied, pos - copied);
        res += "\\x";
        res += hexDigitUppercase(static_cast<UInt8>(value[pos]) / 16);
        res += hexDigitUppercase(static_cast<UInt8>(value[pos]) % 16);
        copied = ++pos;
    }

    /// Fast path, usually there is nothing to escape.
    if (copied == 0)
        return value;

    res.append(value, copied);
    return res;
}

}
