#include <Common/likePatternToRegexp.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}

String likePatternToRegexp(std::string_view pattern)
{
    String res;
    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    if (pos < end && *pos == '%')
        /// Eat leading %
        while (++pos < end)
        {
            if (*pos != '%')
                break;
        }
    else
        res = "^";

    while (pos < end)
    {
        switch (*pos)
        {
            /// Quote characters which have a special meaning in re2
            case '^':
            case '$':
            case '.':
            case '[':
            case '|':
            case '(':
            case ')':
            case '?':
            case '*':
            case '+':
            case '{':
                res += '\\';
                res += *pos;
                break;
            case '%':
                if (pos + 1 != end)
                    res += ".*";
                else
                    return res;
                break;
            case '_':
                res += ".";
                break;
            case '\\':
                if (pos + 1 == end)
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern '{}'", pattern);
                switch (pos[1])
                {
                    /// Interpret quoted LIKE metacharacters %, _ and \ as literals:
                    case '%':
                    case '_':
                        res += pos[1];
                        ++pos;
                        break;
                    case '\\':
                        res += "\\\\"; /// backslash has a special meaning in re2 --> quote it
                        ++pos;
                        break;
                    /// Unknown escape sequence treated literally: as backslash (which must be quoted in re2) + the following character
                    default:
                        res += "\\\\";
                        break;
                }
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    res += '$';
    return res;
}

String similarToPatternToRegexp(std::string_view pattern)
{
    String res;
    res.reserve(pattern.size() * 2 + 5);

    /// Wrap the body in `^(?:...)$`. The non-capturing group is required so that top-level
    /// alternation has the right precedence: `abc|def` becomes `^(?:abc|def)$` (full-string
    /// match of either branch) rather than `^abc|def$` which re2 parses as `(^abc)|(def$)`.
    res = "^(?:";

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    bool in_bracket = false;
    bool maybe_in_class = false;
    while (pos < end)
    {
        /// SIMILAR TO's metacharacters consist of LIKE's and a subset of re2's:
        ///   - LIKE's: %_         --> Convert to .* or . in re2
        ///   - re2's: |*+?[](){}  --> Keep in re2
        ///   - Exclude re2's: ^$. --> Quote in re2
        /// \ always starts an escape sequence
        /// Inside a bracket expression, only ^, \, -, ] are special; all others are literal
        switch (*pos)
        {
            /// Keep unescaped brackets. Remember in bracket or not.
            /// We can avoid lookahead cost for class expression by this following rules:
            /// - [ (not [:) opens a bracket
            /// - [: opens a maybe-class
            /// - :] closes a maybe-class if it's opened, else closes a bracket
            /// - ] closes a maybe-class if it's opened, else closes a bracket
            case '[':
                res += *pos;
                if (pos + 1 < end)
                {
                    switch (pos[1])
                    {
                        /// [: maybe class open
                        case ':':
                            maybe_in_class = true;
                            break;
                        /// [ bracket open
                        default:
                        {
                            in_bracket = true;
                            /// POSIX rule: an `]` immediately after `[` or `[^` is a literal member,
                            /// not the bracket terminator. Emit it as `\]` so re2 keeps the bracket open.
                            size_t lookahead = 1;
                            bool negated = false;
                            if (pos[lookahead] == '^')
                            {
                                negated = true;
                                ++lookahead;
                            }
                            if (pos + lookahead < end && pos[lookahead] == ']')
                            {
                                if (negated)
                                    res += '^';
                                res += "\\]";
                                pos += lookahead;
                            }
                            break;
                        }
                    }
                }
                else
                    in_bracket = true;
                break;
            case ']':
                if (maybe_in_class && pos - 1 > pattern.data())
                {
                    switch (*(pos - 1))
                    {
                        /// :] maybe class close
                        case ':':
                            maybe_in_class = false;
                            break;
                        /// ] bracket close
                        default:
                            maybe_in_class = false;
                            in_bracket = false;
                    }
                }
                else
                    in_bracket = false;
                res += *pos;
                break;
            /// Quote characters which have a special meaning in re2. Don't quote when in bracket.
            case '^':
            case '$':
            case '.':
                if (!in_bracket && !maybe_in_class)
                    res += '\\';
                res += *pos;
                break;
            /// Convert LIKE's metacharacters to re2's. Don't convert when in bracket.
            case '%':
                if (!in_bracket && !maybe_in_class)
                    res += ".*";
                else
                    res += *pos;
                break;
            case '_':
                if (!in_bracket && !maybe_in_class)
                    res += ".";
                else
                    res += *pos;
                break;
            /// Handle escape sequence
            case '\\':
                if (pos + 1 == end)
                    throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of SIMILAR TO pattern '{}'", pattern);
                switch (pos[1])
                {
                    /// Unquote LIKE metacharacters %, _ and \ as literals for re2:
                    case '%':
                    case '_':
                        res += pos[1];
                        ++pos;
                        break;
                    /// Keep escaped SIMILAR TO excluding LIKE metacharacters for re2:
#define CASES(c) case c:
                    SIMILAR_TO_EXCLUDING_LIKE_METACHARS(CASES)
#undef CASES
                        res += '\\';
                        res += pos[1];
                        ++pos;
                        break;
                    /// Quote backslash
                    case '\\':
                        res += "\\\\";
                        ++pos;
                        break;
                    /// Unknown escape sequence treated literally: as backslash (which must be quoted in re2) + the following character
                    default:
                        res += "\\\\";
                        break;
                }
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    res += ")$";
    return res;
}

}
