#include <Analyzer/TableQualifiers.h>

#include <Analyzer/Utils.h>
#include <Common/StringUtils.h>
#include <Common/VectorWithMemoryTracking.h>
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

/// Parse a key written by `PlannerContext::createSetKey` at `at`: `__set_<hash>` or `__set_<type>_<hash>`, where `<hash>`
/// is `<digits>_<digits>`, followed by the `)` that closes the `IN` call. On success, set the offsets of the hash and of that `)`.
bool parseSetKey(const String & name, size_t at, size_t & hash_begin, size_t & hash_end)
{
    static constexpr std::string_view prefix = "__set_";
    if (name.compare(at, prefix.size(), prefix) != 0)
        return false;

    auto skip_digits = [&](size_t begin)
    {
        while (begin < name.size() && isNumericASCII(name[begin]))
            ++begin;
        return begin;
    };
    auto parse_hash = [&](size_t begin)
    {
        const size_t low_end = skip_digits(begin);
        if (low_end == begin || low_end == name.size() || name[low_end] != '_')
            return false;
        const size_t end = skip_digits(low_end + 1);
        if (end == low_end + 1 || end == name.size() || name[end] != ')')
            return false;
        hash_begin = begin;
        hash_end = end;
        return true;
    };

    const size_t key_begin = at + prefix.size();
    if (parse_hash(key_begin))
        return true;

    /// `IDataType::getName` of the left operand: letters and digits, then optionally one balanced `(...)`.
    size_t type_end = key_begin;
    while (type_end < name.size() && isAlphaNumericASCII(name[type_end]))
        ++type_end;
    if (type_end == key_begin)
        return false;

    if (type_end < name.size() && name[type_end] == '(')
    {
        size_t depth = 0;
        do
        {
            const char c = name[type_end];
            if (c == '\'' || c == '`')
            {
                type_end = skipQuotedSpan(name, type_end);
                continue;
            }
            if (c == '(')
                ++depth;
            else if (c == ')')
                --depth;
            ++type_end;
        } while (depth > 0 && type_end < name.size());

        if (depth > 0)
            return false;
    }

    if (type_end == name.size() || name[type_end] != '_')
        return false;
    return parse_hash(type_end + 1);
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

String normalizeGeneratedSetKeys(const String & name)
{
    struct Call
    {
        bool is_in_function = false;
        size_t argument = 0;
    };
    VectorWithMemoryTracking<Call> calls;

    String result;
    result.reserve(name.size());
    size_t pos = 0;
    while (pos < name.size())
    {
        const char c = name[pos];
        if (c == '\'' || c == '`')
        {
            const size_t span_end = skipQuotedSpan(name, pos);
            result.append(name, pos, span_end - pos);
            pos = span_end;
            continue;
        }

        size_t hash_begin = 0;
        size_t hash_end = 0;
        if (!calls.empty() && calls.back().is_in_function && calls.back().argument == 1 && pos >= 2
            && name.compare(pos - 2, 2, ", ") == 0 && parseSetKey(name, pos, hash_begin, hash_end))
        {
            result.append(name, pos, hash_begin - pos);
            pos = hash_end;
            continue;
        }

        if (c == '(')
        {
            size_t function_begin = pos;
            while (function_begin > 0 && isWordCharASCII(name[function_begin - 1]))
                --function_begin;
            calls.push_back({isNameOfInFunction(name.substr(function_begin, pos - function_begin)), 0});
        }
        else if (c == '[')
            calls.push_back({});
        else if (c == ',' && !calls.empty())
            ++calls.back().argument;
        else if ((c == ')' || c == ']') && !calls.empty())
            calls.pop_back();

        result.push_back(c);
        ++pos;
    }
    return result;
}


void writeCacheKeyColumnName(const String & name, const Block * input_header, WriteBuffer & out)
{
    /// `input_header->columns()` is the sentinel for "not a column of this header": it is one past every
    /// valid position, so it can never be confused with one.
    /// Once the header resolves the column, its position IS its identity and the name adds nothing: two
    /// distinct columns of one header sit at distinct positions, and two same-position columns of
    /// different headers are separated by the child's own key. The name only adds a way to disagree - the
    /// shard rewrite can render the same slot as `k` in one plan build and `plus(__table1.a, 1)` in the
    /// other by inlining an alias or a `JOIN USING`, and normalizing erases just the qualifier's index.
    ///
    /// A name the header does not resolve keeps its normalized text, which is then all there is.
    if (input_header && input_header->has(name))
    {
        writeVarUInt(input_header->getPositionByName(name), out);
        return;
    }

    writeVarUInt(input_header ? input_header->columns() : 0, out);
    writeStringBinary(normalizeGeneratedTableQualifiers(name), out);
}

}
