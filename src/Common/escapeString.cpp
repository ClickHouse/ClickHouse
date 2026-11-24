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
        if (width > 0 || width == widechar_ambiguous || width == widechar_widened_in_9)
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
