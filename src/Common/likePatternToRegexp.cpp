#include <Common/likePatternToRegexp.h>

#include <Common/Exception.h>
#include <absl/base/attributes.h>

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
            /// - [: opens a maybe-class, but only when a bracket is already open
            /// - :] closes a maybe-class if it's opened, else closes a bracket
            /// - ] closes a maybe-class if it's opened, else closes a bracket
            case '[':
                res += *pos;
                /// A POSIX character class `[:name:]` only exists *inside* a bracket expression, so a
                /// leading `[:` starts a maybe-class only when a bracket is already open (e.g. the inner
                /// `[:digit:]` of `[[:digit:]]`). A top-level `[:` (e.g. `[:[]`) opens a bracket whose
                /// first member is `:`, not a class.
                if (in_bracket)
                {
                    /// Already inside a bracket expression:
                    /// - `[:` may start a POSIX character class such as `[:digit:]`.
                    /// - any other `[` is a literal member (POSIX/RE2 syntax), not a new bracket open, so
                    ///   we must not run the leading-`]` lookahead below — that would incorrectly consume
                    ///   the outer bracket's closing `]` (e.g. `[[]` would become an unterminated class).
                    if (pos + 1 < end && pos[1] == ':')
                        maybe_in_class = true;
                }
                else
                {
                    /// This `[` opens a bracket expression.
                    in_bracket = true;
                    /// POSIX rule: an `]` immediately after `[` or `[^` is a literal member,
                    /// not the bracket terminator. Emit it as `\]` so re2 keeps the bracket open.
                    size_t lookahead = 1;
                    bool negated = false;
                    if (pos + lookahead < end && pos[lookahead] == '^')
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
                }
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
                    /// Escaped excluded metacharacters ^ $ . denote the literal character, the same as
                    /// their unescaped forms which the translator already quotes for re2. Emit them
                    /// quoted (e.g. `\.` -> `\.`) and consume the metacharacter. Without this case they
                    /// would fall into the generic escape branch below, which emits a quoted backslash
                    /// and re-processes the metacharacter, producing a regexp that matches a literal
                    /// backslash (`\.` -> `\\\.`) instead of the literal character.
                    case '^':
                    case '$':
                    case '.':
                        res += '\\';
                        res += pos[1];
                        ++pos;
                        break;
                    default:
                        if (in_bracket || maybe_in_class)
                        {
                            /// Inside a bracket expression an escaped character is a single literal
                            /// member of the class. Only `-` and `^` are special inside an re2
                            /// character class, so only those are emitted as `\<char>`: `[\-]` matches
                            /// only `-` (not a backslash) and `[\^]` matches a literal `^`. Every other
                            /// character is emitted as-is — in particular re2's Perl classes such as
                            /// `\d`, `\w`, `\s` are not part of the `SIMILAR TO` grammar, so `[\d]` must
                            /// match a literal `d`, not the digit class.
                            if (pos[1] == '-' || pos[1] == '^')
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

template <bool is_similar_to>
bool likePatternIsSubstring(std::string_view pattern, String & res)
{
    /// TODO: ignore multiple leading or trailing %
    if (pattern.size() < 2 || !pattern.starts_with('%') || !pattern.ends_with('%'))
        return false;

    res.clear();
    res.reserve(pattern.size() - 2);

    const char * pos = pattern.data() + 1;
    const char * const end = pattern.data() + pattern.size() - 1;

    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                return false;
            case '\\':
                ++pos;
                if (pos == end)
                    /// pattern ends with \% --> trailing % is to be taken literally and pattern doesn't qualify for substring search
                    return false;

                switch (*pos)
                {
                    /// Known LIKE escape sequences:
                    case '%':
                    case '_':
                    case '\\':
                        res += *pos;
                        break;
                    /// Escaped excluded metacharacters ^ $ . are literal characters in SIMILAR TO,
                    /// the same as their unescaped forms. For [I]LIKE the backslash is not special
                    /// before these characters, so keep the literal backslash (same as the default).
                    case '^':
                    case '$':
                    case '.':
                        if constexpr (is_similar_to)
                            res += *pos;
                        else
                        {
                            res += '\\';
                            res += *pos;
                        }
                        break;
#define CASES(c) case c:
                    SIMILAR_TO_EXCLUDING_LIKE_METACHARS(CASES)
#undef CASES
                        if constexpr (is_similar_to)
                        {
                            res += *pos;
                            break;
                        }
                        else
                            ABSL_FALLTHROUGH_INTENDED;
                    /// For all other escape sequences, the backslash loses its special meaning
                    default:
                        res += '\\';
                        res += *pos;
                        break;
                }

                break;
#define CASES(c) case c:
            SIMILAR_TO_EXCLUDING_LIKE_METACHARS(CASES)
#undef CASES
                /// A SIMILAR TO metacharacter (other than %/_) makes the pattern more than a plain
                /// substring search, so it does not qualify. For [I]LIKE these are ordinary literals.
                if constexpr (is_similar_to)
                    return false;
                else
                    ABSL_FALLTHROUGH_INTENDED;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

template bool likePatternIsSubstring<false>(std::string_view pattern, String & res);
template bool likePatternIsSubstring<true>(std::string_view pattern, String & res);

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

String similarToPatternWithCustomEscapeToSimilarToPattern(std::string_view pattern, char escape_char)
{
    /// Does `\c` denote a literal `c` in the standard (backslash-escape) SIMILAR TO grammar as
    /// processed by `similarToPatternToRegexp`? True for the LIKE metacharacters (`%`, `_`), the
    /// SIMILAR TO metacharacters excluded from LIKE (`| * + ? { } ( ) [ ]`), the characters that
    /// SIMILAR TO always quotes for re2 (`^ $ .`), and backslash itself. For any other character a
    /// leading backslash is not consumed as an escape, so the character is already a literal on its
    /// own and must be emitted unescaped.
    const auto needs_backslash_to_be_literal = [](char c) -> bool
    {
        switch (c)
        {
            case '%':
            case '_':
            case '^':
            case '$':
            case '.':
            case '\\':
                return true;
#define CASES(x) case x:
            SIMILAR_TO_EXCLUDING_LIKE_METACHARS(CASES)
#undef CASES
                return true;
            default:
                return false;
        }
    };

    /// Nothing to rewrite when the custom escape is already the standard backslash escape.
    if (escape_char == '\\')
        return String(pattern);

    String res;
    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * const end = pattern.data() + pattern.size();

    /// Bracket state, tracked exactly as in `similarToPatternToRegexp`. The escape character is only
    /// special *outside* a bracket expression (POSIX / PostgreSQL do not recognize escapes inside
    /// `[...]`), so within a bracket it is passed through verbatim and `similarToPatternToRegexp`
    /// applies the bracket rules. A bare backslash, however, is a literal everywhere under a custom
    /// escape (backslash is no longer the escape) and is rewritten to `\\` inside brackets too —
    /// otherwise the standard translator would consume it as a bracket escape, turning the member
    /// `\` of `[a\]` into an unterminated class and collapsing `[\d]` to `[d]`.
    bool in_bracket = false;
    bool maybe_in_class = false;

    while (pos < end)
    {
        if (!in_bracket && !maybe_in_class && *pos == escape_char)
        {
            ++pos;
            if (pos == end)
                throw Exception(ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE, "Invalid escape sequence at the end of SIMILAR TO pattern '{}'", pattern);

            /// The escape character makes the following character a literal. Represent that literal
            /// in the standard grammar: `\c` for characters that would otherwise be special, or the
            /// bare character otherwise.
            if (needs_backslash_to_be_literal(*pos))
                res += '\\';
            res += *pos;
            ++pos;
            continue;
        }
        if (*pos == '\\')
        {
            /// When a custom escape character is used, a bare backslash is a literal (backslash is no
            /// longer the escape). Emit `\\` so `similarToPatternToRegexp` keeps it as a literal
            /// backslash — `\\` denotes a literal backslash member inside bracket expressions as well.
            res += "\\\\";
            ++pos;
            continue;
        }

        /// Not an escape: update bracket state and emit the character verbatim.
        switch (*pos)
        {
            case '[':
                res += *pos;
                if (in_bracket)
                {
                    /// A `[:` inside a bracket may start a POSIX class such as `[:digit:]`.
                    if (pos + 1 < end && pos[1] == ':')
                        maybe_in_class = true;
                }
                else
                {
                    in_bracket = true;
                    /// A `]` immediately after `[` or `[^` is a literal member, not the terminator.
                    /// Emit it verbatim and keep the bracket open (the translator re-quotes it for re2).
                    size_t lookahead = 1;
                    if (pos + lookahead < end && pos[lookahead] == '^')
                    {
                        res += '^';
                        ++lookahead;
                    }
                    if (pos + lookahead < end && pos[lookahead] == ']')
                    {
                        res += ']';
                        pos += lookahead;
                    }
                }
                break;
            case ']':
                if (maybe_in_class && pos - 1 > pattern.data())
                {
                    if (*(pos - 1) == ':')
                        maybe_in_class = false;
                    else
                    {
                        maybe_in_class = false;
                        in_bracket = false;
                    }
                }
                else
                    in_bracket = false;
                res += *pos;
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return res;
}

}
