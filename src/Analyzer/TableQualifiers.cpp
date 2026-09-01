#include <Analyzer/TableQualifiers.h>

#include <Common/StringUtils.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{

/// `name[at]` opens a quoted span (a string constant `'...'` or a backquoted identifier `` `...` ``). Return the
/// offset just past the closing quote, or `name.size()` if unterminated. A backslash escapes the next character
/// (including the quote and another backslash), matching `writeAnyEscapedString` (the escaping used by both string
/// constants and backquoted identifiers), so the span ends at the first unescaped matching quote.
size_t skipQuotedSpan(const String & name, size_t at)
{
    const char quote = name[at];
    size_t pos = at + 1;
    while (pos < name.size())
    {
        if (name[pos] == '\\')
            pos += 2; /// Skip the backslash and the character it escapes.
        else if (name[pos] == quote)
            return pos + 1;
        else
            ++pos;
    }
    return name.size();
}

}

String normalizeGeneratedTableQualifiers(const String & name, const std::unordered_set<String> * genuine_tails)
{
    static constexpr std::string_view prefix = "__table";
    String result;
    result.reserve(name.size());
    size_t pos = 0;
    while (pos < name.size())
    {
        if (name[pos] == '\'' || name[pos] == '`')
        {
            /// Copy the quoted span verbatim: its contents are user text, never an analyzer qualifier.
            size_t span_end = skipQuotedSpan(name, pos);
            result.append(name, pos, span_end - pos);
            pos = span_end;
            continue;
        }

        bool boundary = pos == 0 || !isWordCharASCII(name[pos - 1]);
        if (boundary && name.compare(pos, prefix.size(), prefix) == 0)
        {
            size_t digit_begin = pos + prefix.size();
            size_t digit_end = digit_begin;
            while (digit_end < name.size() && isNumericASCII(name[digit_end]))
                ++digit_end;
            if (digit_end > digit_begin && digit_end < name.size() && name[digit_end] == '.')
            {
                /// Read the tail right after the dot. It is the column name as `buildColumnIdentifier` renders it:
                /// either a backquoted span (`` `col` `` for a column whose name needs quoting, e.g. one containing a
                /// dot) or a bare run of identifier characters. With `genuine_tails` given, the qualifier is genuine
                /// only when that tail is a known column name from the query tree.
                size_t tail_begin = digit_end + 1;
                size_t tail_end = tail_begin;
                if (tail_begin < name.size() && name[tail_begin] == '`')
                    tail_end = skipQuotedSpan(name, tail_begin);
                else
                    while (tail_end < name.size() && isWordCharASCII(name[tail_end]))
                        ++tail_end;
                const bool tail_is_genuine = tail_end > tail_begin
                    && (!genuine_tails || genuine_tails->contains(name.substr(tail_begin, tail_end - tail_begin)));
                if (tail_is_genuine)
                {
                    /// Genuine qualifier `__table<digits>.<tail>`: copy `__table` and the `.`, dropping the digits.
                    result.append(prefix);
                    result.push_back('.');
                    pos = digit_end + 1;
                    continue;
                }
            }
        }
        result.push_back(name[pos]);
        ++pos;
    }
    return result;
}


void writeCacheKeyColumnName(const String & name, const Block * input_header, WriteBuffer & out)
{
    /// `input_header->columns()` is the sentinel for "not a column of this header": it is one past every
    /// valid position, so it can never be confused with one.
    const size_t position = (input_header && input_header->has(name)) ? input_header->getPositionByName(name)
                                                                     : (input_header ? input_header->columns() : 0);
    writeVarUInt(position, out);
    writeStringBinary(normalizeGeneratedTableQualifiers(name), out);
}

}
