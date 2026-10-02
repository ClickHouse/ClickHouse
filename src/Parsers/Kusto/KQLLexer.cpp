#include <Parsers/Kusto/KQLLexer.h>

#include <Common/StringUtils.h>
#include <base/arithmeticOverflow.h>
#include <base/hex.h>

#include <algorithm>
#include <array>
#include <cmath>


namespace DB
{

namespace
{

constexpr Int64 TICKS_PER_MICROSECOND = 10;
constexpr Int64 TICKS_PER_MILLISECOND = 1000 * TICKS_PER_MICROSECOND;
constexpr Int64 TICKS_PER_SECOND = 1000 * TICKS_PER_MILLISECOND;
constexpr Int64 TICKS_PER_MINUTE = 60 * TICKS_PER_SECOND;
constexpr Int64 TICKS_PER_HOUR = 60 * TICKS_PER_MINUTE;
constexpr Int64 TICKS_PER_DAY = 24 * TICKS_PER_HOUR;

struct TimespanUnit
{
    std::string_view suffix;
    Int64 ticks;
};

/// Longest first: `ms` must win over `m`, `microseconds` over `microsecond`.
/// Note that in KQL `m` is *minutes*, not months - there is no month unit.
constexpr std::array TIMESPAN_UNITS{
    TimespanUnit{"microseconds", TICKS_PER_MICROSECOND},
    TimespanUnit{"microsecond", TICKS_PER_MICROSECOND},
    TimespanUnit{"milliseconds", TICKS_PER_MILLISECOND},
    TimespanUnit{"millisecond", TICKS_PER_MILLISECOND},
    TimespanUnit{"nanoseconds", 0}, /// Sub-tick; handled as a fraction of a tick below.
    TimespanUnit{"nanosecond", 0},
    TimespanUnit{"seconds", TICKS_PER_SECOND},
    TimespanUnit{"second", TICKS_PER_SECOND},
    TimespanUnit{"minutes", TICKS_PER_MINUTE},
    TimespanUnit{"minute", TICKS_PER_MINUTE},
    TimespanUnit{"hours", TICKS_PER_HOUR},
    TimespanUnit{"hour", TICKS_PER_HOUR},
    TimespanUnit{"days", TICKS_PER_DAY},
    TimespanUnit{"day", TICKS_PER_DAY},
    TimespanUnit{"ticks", 1},
    TimespanUnit{"tick", 1},
    TimespanUnit{"ms", TICKS_PER_MILLISECOND},
    TimespanUnit{"d", TICKS_PER_DAY},
    TimespanUnit{"h", TICKS_PER_HOUR},
    TimespanUnit{"m", TICKS_PER_MINUTE},
    TimespanUnit{"s", TICKS_PER_SECOND},
};

bool isIdentifierStart(char c)
{
    return isAlphaASCII(c) || c == '_';
}

bool isIdentifierChar(char c)
{
    return isAlphaNumericASCII(c) || c == '_';
}

}

Int64 kqlTimespanUnitInTicks(std::string_view unit)
{
    for (const auto & candidate : TIMESPAN_UNITS)
        if (candidate.suffix == unit)
            return candidate.ticks;
    return 0;
}

std::optional<Int64> kqlParseTimespanText(std::string_view text)
{
    bool negative = false;
    if (!text.empty() && (text.front() == '-' || text.front() == '+'))
    {
        negative = text.front() == '-';
        text.remove_prefix(1);
    }

    std::vector<std::string_view> parts;
    size_t start = 0;
    for (size_t i = 0; i <= text.size(); ++i)
    {
        if (i == text.size() || text[i] == ':')
        {
            parts.push_back(text.substr(start, i - start));
            start = i + 1;
        }
    }
    if (parts.size() != 3 && parts.size() != 1)
        return {};

    const auto to_number = [](std::string_view part, Int64 & out)
    {
        if (part.empty() || part.size() > 18)
            return false;
        out = 0;
        for (const char c : part)
        {
            if (c < '0' || c > '9')
                return false;
            out = out * 10 + (c - '0');
        }
        return true;
    };

    Int64 days = 0;
    Int64 hours = 0;
    Int64 minutes = 0;
    Int64 seconds = 0;
    Int64 ticks = 0;

    /// The first part may carry a leading `d.`, and the last a trailing `.fffffff`.
    std::string_view head = parts.front();
    if (const size_t dot = head.find('.'); dot != std::string_view::npos)
    {
        if (!to_number(head.substr(0, dot), days))
            return {};
        head = head.substr(dot + 1);
    }

    if (parts.size() == 1)
    {
        /// `time('2')` is not a thing; a bare component must have come with `d.`.
        if (days == 0)
            return {};
        if (!head.empty() && !to_number(head, hours))
            return {};
    }
    else
    {
        if (!to_number(head, hours))
            return {};
        if (!to_number(parts[1], minutes))
            return {};

        std::string_view tail = parts[2];
        if (const size_t dot = tail.find('.'); dot != std::string_view::npos)
        {
            std::string fraction(tail.substr(dot + 1));
            if (fraction.size() > 7)
                fraction.resize(7);
            fraction.resize(7, '0');
            if (!to_number(fraction, ticks))
                return {};
            tail = tail.substr(0, dot);
        }
        if (!to_number(tail, seconds))
            return {};
    }

    /// Every component is a field of up to 18 digits of untrusted text, so each step is
    /// checked: an oversized literal must fail cleanly instead of overflowing.
    Int64 total = 0;
    Int64 result = 0;
    if (common::mulOverflow<Int64>(days, 24, total) || common::addOverflow<Int64>(total, hours, total)
        || common::mulOverflow<Int64>(total, 60, total) || common::addOverflow<Int64>(total, minutes, total)
        || common::mulOverflow<Int64>(total, 60, total) || common::addOverflow<Int64>(total, seconds, total)
        || common::mulOverflow<Int64>(total, 10'000'000, result) || common::addOverflow<Int64>(result, ticks, result))
        return {};
    /// All components are non-negative here, so `result` is too and the negation cannot overflow.
    return negative ? -result : result;
}

const char * getKQLTokenName(KQLTokenType type)
{
    switch (type)
    {
        case KQLTokenType::BareWord: return "identifier";
        case KQLTokenType::Number: return "number";
        case KQLTokenType::StringLiteral: return "string literal";
        case KQLTokenType::Timespan: return "timespan literal";
        case KQLTokenType::DateTimeLiteral: return "datetime literal";
        case KQLTokenType::GuidLiteral: return "guid literal";
        case KQLTokenType::Pipe: return "'|'";
        case KQLTokenType::Comma: return "','";
        case KQLTokenType::Semicolon: return "';'";
        case KQLTokenType::Dot: return "'.'";
        case KQLTokenType::DotDot: return "'..'";
        case KQLTokenType::Colon: return "':'";
        case KQLTokenType::OpeningRoundBracket: return "'('";
        case KQLTokenType::ClosingRoundBracket: return "')'";
        case KQLTokenType::OpeningSquareBracket: return "'['";
        case KQLTokenType::ClosingSquareBracket: return "']'";
        case KQLTokenType::OpeningCurlyBrace: return "'{'";
        case KQLTokenType::ClosingCurlyBrace: return "'}'";
        case KQLTokenType::Plus: return "'+'";
        case KQLTokenType::Minus: return "'-'";
        case KQLTokenType::Asterisk: return "'*'";
        case KQLTokenType::Slash: return "'/'";
        case KQLTokenType::Percent: return "'%'";
        case KQLTokenType::Equals: return "'='";
        case KQLTokenType::DoubleEquals: return "'=='";
        case KQLTokenType::NotEquals: return "'!='";
        case KQLTokenType::Less: return "'<'";
        case KQLTokenType::Greater: return "'>'";
        case KQLTokenType::LessOrEquals: return "'<='";
        case KQLTokenType::GreaterOrEquals: return "'>='";
        case KQLTokenType::TildeEquals: return "'=~'";
        case KQLTokenType::NotTildeEquals: return "'!~'";
        case KQLTokenType::EndOfStream: return "end of query";
        case KQLTokenType::Error: return "invalid token";
    }
    return "unknown token";
}

std::vector<KQLToken> KQLLexer::tokenize()
{
    std::vector<KQLToken> tokens;
    while (true)
    {
        KQLToken token = nextToken();
        const bool stop = token.isEnd() || token.isError();
        tokens.push_back(std::move(token));
        if (stop)
            break;
    }
    return tokens;
}

KQLToken KQLLexer::makeToken(KQLTokenType type, const char * token_begin) const
{
    KQLToken token;
    token.type = type;
    token.begin = token_begin;
    token.end = pos;
    return token;
}

KQLToken KQLLexer::makeError(const char * token_begin, String reason) const
{
    KQLToken token;
    token.type = KQLTokenType::Error;
    token.begin = token_begin;
    /// Always cover at least one character so the caret in the error message has something to point at.
    token.end = std::max(pos, std::min(token_begin + 1, end));
    token.inner = std::move(reason);
    return token;
}

/// True when only whitespace separates `pos` from the start of its line.
bool KQLLexer::atLineStart() const
{
    for (const char * back = pos; back > begin;)
    {
        --back;
        if (*back == '\n')
            return true;
        if (!isWhitespaceASCII(*back))
            return false;
    }
    return true;
}

void KQLLexer::skipWhitespaceAndComments()
{
    while (pos < end)
    {
        if (isWhitespaceASCII(*pos))
        {
            ++pos;
        }
        else if (*pos == '/' && pos + 1 < end && pos[1] == '/')
        {
            /// KQL line comment. There is no block comment in KQL.
            while (pos < end && *pos != '\n')
                ++pos;
        }
        else if (*pos == '-' && pos + 1 < end && pos[1] == '-' && atLineStart())
        {
            /// `--` is not a KQL comment, but it is how every surrounding tool - test files,
            /// clients that prepend a banner - writes one. Accepting it only when the line
            /// holds nothing else keeps `1--1` arithmetic, which Kusto reads as `1 - (-1)`.
            while (pos < end && *pos != '\n')
                ++pos;
        }
        else
        {
            break;
        }
    }
}

KQLToken KQLLexer::nextToken()
{
    skipWhitespaceAndComments();

    const char * const token_begin = pos;
    if (pos >= end)
        return makeToken(KQLTokenType::EndOfStream, token_begin);

    const char c = *pos;

    if (isNumericASCII(c))
        return lexNumberOrTimespan(token_begin);

    if (c == '\'' || c == '"')
    {
        ++pos;
        return lexString(token_begin, c, /*verbatim=*/false);
    }

    /// Verbatim string: `@'a\b'` keeps the backslash. Doubling the quote escapes it.
    if (c == '@' && pos + 1 < end && (pos[1] == '\'' || pos[1] == '"'))
    {
        const char quote = pos[1];
        pos += 2;
        return lexString(token_begin, quote, /*verbatim=*/true);
    }

    if (isIdentifierStart(c))
        return lexBareWord(token_begin);

    /// `$left` / `$right` in a join condition - the only `$`-prefixed names KQL has.
    if (c == '$' && pos + 1 < end && isIdentifierStart(pos[1]))
    {
        ++pos;
        return lexBareWord(token_begin);
    }

    /// `!in`, `!contains`, `!startswith`, ... - one token, because `!` is not a general
    /// prefix operator in KQL (that spelling is `not`).
    if (c == '!' && pos + 1 < end && isIdentifierStart(pos[1]))
    {
        ++pos;
        while (pos < end && isIdentifierChar(*pos))
            ++pos;
        if (pos < end && *pos == '~')
            ++pos;
        return makeToken(KQLTokenType::BareWord, token_begin);
    }

    ++pos;
    const auto peek = [&](char expected) { return pos < end && *pos == expected; };

    switch (c)
    {
        case '|': return makeToken(KQLTokenType::Pipe, token_begin);
        case ',': return makeToken(KQLTokenType::Comma, token_begin);
        case ';': return makeToken(KQLTokenType::Semicolon, token_begin);
        case ':': return makeToken(KQLTokenType::Colon, token_begin);
        case '(': return makeToken(KQLTokenType::OpeningRoundBracket, token_begin);
        case ')': return makeToken(KQLTokenType::ClosingRoundBracket, token_begin);
        case '[': return makeToken(KQLTokenType::OpeningSquareBracket, token_begin);
        case ']': return makeToken(KQLTokenType::ClosingSquareBracket, token_begin);
        case '{': return makeToken(KQLTokenType::OpeningCurlyBrace, token_begin);
        case '}': return makeToken(KQLTokenType::ClosingCurlyBrace, token_begin);
        case '+': return makeToken(KQLTokenType::Plus, token_begin);
        case '*': return makeToken(KQLTokenType::Asterisk, token_begin);
        case '/': return makeToken(KQLTokenType::Slash, token_begin);
        case '%': return makeToken(KQLTokenType::Percent, token_begin);
        case '-': return makeToken(KQLTokenType::Minus, token_begin);

        case '.':
        {
            if (peek('.'))
            {
                ++pos;
                return makeToken(KQLTokenType::DotDot, token_begin);
            }
            /// `.5` is a number, `.` alone is member access.
            if (pos < end && isNumericASCII(*pos))
            {
                pos = token_begin;
                return lexNumberOrTimespan(token_begin);
            }
            return makeToken(KQLTokenType::Dot, token_begin);
        }

        case '=':
        {
            if (peek('='))
            {
                ++pos;
                return makeToken(KQLTokenType::DoubleEquals, token_begin);
            }
            if (peek('~'))
            {
                ++pos;
                return makeToken(KQLTokenType::TildeEquals, token_begin);
            }
            return makeToken(KQLTokenType::Equals, token_begin);
        }

        case '!':
        {
            if (peek('='))
            {
                ++pos;
                return makeToken(KQLTokenType::NotEquals, token_begin);
            }
            if (peek('~'))
            {
                ++pos;
                return makeToken(KQLTokenType::NotTildeEquals, token_begin);
            }
            return makeError(token_begin, "'!' must be followed by '=', '~' or an operator name such as 'in'");
        }

        case '<':
        {
            if (peek('='))
            {
                ++pos;
                return makeToken(KQLTokenType::LessOrEquals, token_begin);
            }
            if (peek('>'))
            {
                ++pos;
                return makeToken(KQLTokenType::NotEquals, token_begin);
            }
            return makeToken(KQLTokenType::Less, token_begin);
        }

        case '>':
        {
            if (peek('='))
            {
                ++pos;
                return makeToken(KQLTokenType::GreaterOrEquals, token_begin);
            }
            return makeToken(KQLTokenType::Greater, token_begin);
        }

        default:
            break;
    }

    return makeError(token_begin, "unexpected character");
}

KQLToken KQLLexer::lexBareWord(const char * token_begin)
{
    while (pos < end && isIdentifierChar(*pos))
        ++pos;

    /// `in~` is the case-insensitive form of `in`, written with no space.
    if (pos < end && *pos == '~')
        ++pos;

    const std::string_view word{token_begin, static_cast<size_t>(pos - token_begin)};

    /// `datetime(...)` and `guid(...)` wrap text that the ordinary rules would take apart:
    /// `2020-01-01` is not three numbers separated by minus signs. Capture it whole.
    if (pos < end && *pos == '(')
    {
        if (word == "datetime")
            return lexParenthesizedLiteral(token_begin, KQLTokenType::DateTimeLiteral);
        if (word == "guid")
            return lexParenthesizedLiteral(token_begin, KQLTokenType::GuidLiteral);
    }

    return makeToken(KQLTokenType::BareWord, token_begin);
}

KQLToken KQLLexer::lexParenthesizedLiteral(const char * token_begin, KQLTokenType type)
{
    ++pos; /// The '('.
    const char * const inner_begin = pos;

    while (pos < end && *pos != ')')
    {
        /// A newline inside means the closing parenthesis was forgotten; stop before running
        /// to the end of a multi-statement script.
        if (*pos == '\n')
            break;
        ++pos;
    }

    if (pos >= end || *pos != ')')
        return makeError(token_begin, "unterminated literal: expected ')'");

    const char * const inner_end = pos;
    ++pos; /// The ')'.

    KQLToken token = makeToken(type, token_begin);
    token.inner.assign(inner_begin, inner_end);

    /// `datetime(null)` is how KQL spells a null datetime.
    while (!token.inner.empty() && isWhitespaceASCII(token.inner.front()))
        token.inner.erase(token.inner.begin());
    while (!token.inner.empty() && isWhitespaceASCII(token.inner.back()))
        token.inner.pop_back();

    /// Strip optional quotes: both `datetime(2020-01-01)` and `datetime("2020-01-01")` are legal.
    if (token.inner.size() >= 2 && (token.inner.front() == '\'' || token.inner.front() == '"')
        && token.inner.back() == token.inner.front())
        token.inner = token.inner.substr(1, token.inner.size() - 2);

    if (token.inner.empty())
        return makeError(token_begin, "empty literal");

    return token;
}

KQLToken KQLLexer::lexNumberOrTimespan(const char * token_begin)
{
    if (pos + 1 < end && *pos == '0' && (pos[1] == 'x' || pos[1] == 'X'))
    {
        pos += 2;
        const char * const digits_begin = pos;
        while (pos < end && isHexDigit(*pos))
            ++pos;
        if (pos == digits_begin)
            return makeError(token_begin, "hexadecimal literal has no digits");
        /// A hexadecimal literal never carries a timespan suffix.
        if (pos < end && isIdentifierChar(*pos))
            return makeError(token_begin, "invalid character after a hexadecimal literal");
        return makeToken(KQLTokenType::Number, token_begin);
    }

    while (pos < end && isNumericASCII(*pos))
        ++pos;

    bool is_floating_point = false;
    /// `1..2` is a range of two integers, not the float `1.` followed by `.2`.
    if (pos < end && *pos == '.' && !(pos + 1 < end && pos[1] == '.'))
    {
        is_floating_point = true;
        ++pos;
        while (pos < end && isNumericASCII(*pos))
            ++pos;
    }

    const char * const mantissa_end = pos;

    if (pos < end && (*pos == 'e' || *pos == 'E'))
    {
        const char * const exponent_begin = pos;
        ++pos;
        if (pos < end && (*pos == '+' || *pos == '-'))
            ++pos;
        const char * const exponent_digits_begin = pos;
        while (pos < end && isNumericASCII(*pos))
            ++pos;
        if (pos == exponent_digits_begin)
        {
            /// Not an exponent after all - `1e` could still be a timespan if `e` were a unit.
            /// It is not, so rewind and let the suffix check below produce the error.
            pos = exponent_begin;
        }
        else
        {
            /// An exponent makes the literal a plain number - a timespan suffix cannot follow it,
            /// so `is_floating_point`, which only the timespan branch below reads, stays as it is.
            if (pos < end && isIdentifierChar(*pos))
                return makeError(token_begin, "invalid character after a numeric literal");
            return makeToken(KQLTokenType::Number, token_begin);
        }
    }

    /// A timespan is a number glued to a unit: `1d`, `2.5h`, `500ms`.
    if (pos < end && isIdentifierStart(*pos))
    {
        const char * const suffix_begin = pos;
        while (pos < end && isIdentifierChar(*pos))
            ++pos;
        const std::string_view suffix{suffix_begin, static_cast<size_t>(pos - suffix_begin)};

        const bool is_nanosecond = suffix == "nanosecond" || suffix == "nanoseconds";
        const Int64 unit_ticks = kqlTimespanUnitInTicks(suffix);
        if (unit_ticks == 0 && !is_nanosecond)
            return makeError(token_begin, "'" + String(suffix) + "' is not a timespan unit");

        const String mantissa_text(token_begin, mantissa_end);
        KQLToken token = makeToken(KQLTokenType::Timespan, token_begin);

        if (is_floating_point)
        {
            const double mantissa = std::stod(mantissa_text);
            /// A tick is 100 ns, so nanoseconds are a tenth of one.
            const double ticks = is_nanosecond ? mantissa / 100.0 : mantissa * static_cast<double>(unit_ticks);
            if (!std::isfinite(ticks) || std::abs(ticks) > 9.2e18)
                return makeError(token_begin, "timespan literal is out of range");
            token.timespan_ticks = static_cast<Int64>(std::llround(ticks));
        }
        else
        {
            Int64 mantissa = 0;
            for (const char digit : mantissa_text)
            {
                /// Overflow here would silently wrap into a plausible-looking timespan.
                if (mantissa > (std::numeric_limits<Int64>::max() - (digit - '0')) / 10)
                    return makeError(token_begin, "timespan literal is out of range");
                mantissa = mantissa * 10 + (digit - '0');
            }
            if (is_nanosecond)
            {
                token.timespan_ticks = mantissa / 100;
            }
            else
            {
                if (unit_ticks != 0 && mantissa > std::numeric_limits<Int64>::max() / unit_ticks)
                    return makeError(token_begin, "timespan literal is out of range");
                token.timespan_ticks = mantissa * unit_ticks;
            }
        }

        return token;
    }

    if (pos < end && isIdentifierChar(*pos))
        return makeError(token_begin, "invalid character after a numeric literal");

    return makeToken(KQLTokenType::Number, token_begin);
}

KQLToken KQLLexer::lexString(const char * token_begin, char quote, bool verbatim)
{
    String value;

    while (pos < end)
    {
        const char c = *pos;

        if (c == quote)
        {
            /// In a verbatim string the quote is escaped by doubling it.
            if (verbatim && pos + 1 < end && pos[1] == quote)
            {
                value += quote;
                pos += 2;
                continue;
            }
            ++pos;
            KQLToken token = makeToken(KQLTokenType::StringLiteral, token_begin);
            token.inner = std::move(value);
            return token;
        }

        if (c == '\n')
            return makeError(token_begin, "unterminated string literal");

        if (!verbatim && c == '\\')
        {
            ++pos;
            if (pos >= end)
                return makeError(token_begin, "unterminated string literal");

            const char escaped = *pos;
            ++pos;
            switch (escaped)
            {
                case 'n': value += '\n'; break;
                case 'r': value += '\r'; break;
                case 't': value += '\t'; break;
                case '0': value += '\0'; break;
                case '\\': value += '\\'; break;
                case '\'': value += '\''; break;
                case '"': value += '"'; break;
                case 'u':
                {
                    /// `\uXXXX`, encoded as UTF-8.
                    if (pos + 4 > end)
                        return makeError(token_begin, "incomplete \\u escape in a string literal");
                    UInt32 code_point = 0;
                    for (int i = 0; i < 4; ++i)
                    {
                        if (!isHexDigit(pos[i]))
                            return makeError(token_begin, "invalid \\u escape in a string literal");
                        code_point = code_point * 16 + unhex(pos[i]);
                    }
                    pos += 4;

                    if (code_point < 0x80)
                    {
                        value += static_cast<char>(code_point);
                    }
                    else if (code_point < 0x800)
                    {
                        value += static_cast<char>(0xC0 | (code_point >> 6));
                        value += static_cast<char>(0x80 | (code_point & 0x3F));
                    }
                    else
                    {
                        value += static_cast<char>(0xE0 | (code_point >> 12));
                        value += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
                        value += static_cast<char>(0x80 | (code_point & 0x3F));
                    }
                    break;
                }
                default:
                    /// Kusto asks for a verbatim string (`@'...'`) around a regex, but plain
                    /// strings carrying `\w` or `\d` are common. Keep the backslash rather
                    /// than rejecting, so the regex reaches the engine intact.
                    value += '\\';
                    value += escaped;
                    break;
            }
            continue;
        }

        value += c;
        ++pos;
    }

    return makeError(token_begin, "unterminated string literal");
}

}
