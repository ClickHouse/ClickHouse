#include <Common/likePatternToRegexp.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int BAD_ARGUMENTS;
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
                            /// If we are already inside a bracket expression, `[` is a literal
                            /// member (POSIX/RE2 syntax), not a new bracket open. Do not run the
                            /// leading-`]` lookahead here — that would incorrectly consume the
                            /// outer bracket's closing `]` (e.g. `[[]` would become an unterminated
                            /// character class).
                            if (in_bracket)
                                break;
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
            /// Reject re2 extension groups such as `(?:...)`, `(?i:...)`. In SIMILAR TO, `(` only
            /// opens a group and `?` is a quantifier that must follow an atom, so `(?` is not valid.
            /// If passed through unchanged, re2 would interpret it as a flag/extension group and could
            /// silently change matching semantics (e.g. `(?i:...)` enabling case-insensitive matching).
            case '(':
                if (!in_bracket && !maybe_in_class && pos + 1 < end && pos[1] == '?')
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid SIMILAR TO pattern '{}': '(?' is not allowed", pattern);
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
                    default:
                        if (in_bracket || maybe_in_class)
                        {
                            /// Inside a bracket expression an escaped character is a single literal
                            /// member of the class. Emit `\<char>` so that, for example, `[\-]` matches
                            /// only `-` (not a backslash), and `[\^]` matches a literal `^`.
                            res += '\\';
                            res += pos[1];
                            ++pos;
                        }
                        else
                        {
                            /// Unknown escape sequence treated literally: as backslash (which must be quoted in re2) + the following character
                            res += "\\\\";
                        }
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

String likePatternWithCustomEscapeToLikePattern(std::string_view pattern, char escape_char)
{
    String res;
    res.reserve(pattern.size());

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    while (pos < end)
    {
        if (*pos == escape_char)
        {
            ++pos;
            if (pos == end)
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of LIKE pattern '{}'", pattern);

            if (*pos == '%' || *pos == '_')
            {
                /// escape_char + wildcard → \wildcard
                res += '\\';
                res += *pos;
            }
            else if (*pos == escape_char)
            {
                /// escape_char + escape_char → literal escape_char
                /// If escape_char is backslash, emit \\ (standard LIKE escape for literal backslash).
                /// Otherwise, just emit the escape_char as a regular character.
                if (escape_char == '\\')
                    res += "\\\\";
                else
                    res += *pos;
            }
            else if (escape_char == '\\')
            {
                /// Preserve legacy LIKE behavior for the default backslash escape: an unknown
                /// escape sequence is kept as literal backslash + the next character, matching
                /// `likePatternToRegexp` and ensuring that `LIKE p` and `LIKE p ESCAPE '\\'`
                /// stay equivalent for users who only explicitly state the default escape.
                res += '\\';
                res += *pos;
            }
            else
            {
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE,
                    "Invalid escape sequence '{}{}' in LIKE pattern '{}': "
                    "the escape character must be followed by '%', '_', or the escape character itself",
                    escape_char, *pos, pattern);
            }
        }
        else if (*pos == '\\' && escape_char != '\\')
        {
            /// When a custom escape char is used, bare backslashes are literals
            res += "\\\\";
        }
        else
        {
            res += *pos;
        }
        ++pos;
    }

    return res;
}

}
