#include <Parsers/LogsQL/LogsQLLexer.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Poco/String.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

bool isWordChar(char c)
{
    /// Deviation from VictoriaLogs: any non-ASCII (UTF-8 continuation or start) byte is treated as a word character.
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || static_cast<unsigned char>(c) >= 0x80;
}

bool isWhitespaceChar(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

int hexDigit(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return -1;
}

void appendUTF8(String & out, UInt32 code_point)
{
    if (code_point < 0x80)
    {
        out += static_cast<char>(code_point);
    }
    else if (code_point < 0x800)
    {
        out += static_cast<char>(0xC0 | (code_point >> 6));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else if (code_point < 0x10000)
    {
        out += static_cast<char>(0xE0 | (code_point >> 12));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else
    {
        out += static_cast<char>(0xF0 | (code_point >> 18));
        out += static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
}

/// The glue characters which are allowed inside unquoted compound tokens.
constexpr std::string_view glue_compound_tokens[] = {"+", "-", "/", ":", ".", "$"};

/// Compound tokens cannot start with these tokens if they directly abut the previous word.
constexpr std::string_view denied_first_compound_tokens[] = {"/", ".", "$"};

}

LogsQLLexer::LogsQLLexer(const char * begin_, const char * end_)
    : begin(begin_), end(end_), current(begin_), token_begin(begin_)
{
    nextToken();
    /// The first token has no previous token.
    prev_raw_token.clear();
}

void LogsQLLexer::throwSyntaxError(const String & message) const
{
    throw Exception(ErrorCodes::SYNTAX_ERROR, "{}; context: [{}]", message, context());
}

String LogsQLLexer::context() const
{
    const char * context_end = token_begin < end ? token_begin : end;
    const char * context_begin = begin;
    if (context_end - context_begin > 50)
        context_begin = context_end - 50;
    return String(context_begin, context_end);
}

bool LogsQLLexer::isWord(std::string_view text)
{
    if (text.empty())
        return false;
    for (char c : text)
        if (!isWordChar(c))
            return false;
    return true;
}

bool LogsQLLexer::isKeyword(std::string_view keyword) const
{
    if (quoted)
        return false;
    if (token.size() != keyword.size())
        return false;
    for (size_t i = 0; i < token.size(); ++i)
        if (toLowerASCII(token[i]) != keyword[i])
            return false;
    return true;
}

bool LogsQLLexer::isKeywordAny(const std::vector<std::string_view> & keywords) const
{
    for (const auto & keyword : keywords)
        if (isKeyword(keyword))
            return true;
    return false;
}

bool LogsQLLexer::isQueryPartTrailer() const
{
    return isEnd() || isKeyword("|") || isKeyword(")") || isKeyword(";");
}

LogsQLLexer::State LogsQLLexer::backupState() const
{
    return State{current, token_begin, token, raw_token, prev_raw_token, quoted, skipped_space};
}

void LogsQLLexer::restoreState(const State & state)
{
    current = state.current;
    token_begin = state.token_begin;
    token = state.token;
    raw_token = state.raw_token;
    prev_raw_token = state.prev_raw_token;
    quoted = state.quoted;
    skipped_space = state.skipped_space;
}

void LogsQLLexer::checkPrevAdjacentToken(const std::vector<std::string_view> & allowed) const
{
    if (skipped_space || prev_raw_token.empty())
        return;

    String prev_lower = Poco::toLower(prev_raw_token);
    for (const auto & candidate : allowed)
        if (prev_lower == candidate)
            return;

    throwSyntaxError(fmt::format(
        "missing whitespace or ':' between {} and {}; probably, the whole string must be put into quotes",
        prev_raw_token, token));
}

String LogsQLLexer::decodeDoubleQuoted(const char *& pos) const
{
    /// Go interpreted string literal syntax.
    char quote = *pos;
    ++pos;
    String result;
    while (true)
    {
        if (pos == end || *pos == '\n')
            throwSyntaxError("unterminated quoted string");
        char c = *pos;
        if (c == quote)
        {
            ++pos;
            return result;
        }
        if (c != '\\')
        {
            result += c;
            ++pos;
            continue;
        }

        ++pos;
        if (pos == end)
            throwSyntaxError("unterminated escape sequence in quoted string");
        char e = *pos;
        switch (e)
        {
            case 'a': result += '\a'; ++pos; break;
            case 'b': result += '\b'; ++pos; break;
            case 'f': result += '\f'; ++pos; break;
            case 'n': result += '\n'; ++pos; break;
            case 'r': result += '\r'; ++pos; break;
            case 't': result += '\t'; ++pos; break;
            case 'v': result += '\v'; ++pos; break;
            case '\\': result += '\\'; ++pos; break;
            case 'x':
            {
                ++pos;
                if (end - pos < 2 || hexDigit(pos[0]) < 0 || hexDigit(pos[1]) < 0)
                    throwSyntaxError("invalid \\x escape sequence in quoted string");
                result += static_cast<char>(hexDigit(pos[0]) * 16 + hexDigit(pos[1]));
                pos += 2;
                break;
            }
            case 'u':
            case 'U':
            {
                size_t digits = e == 'u' ? 4 : 8;
                ++pos;
                if (static_cast<size_t>(end - pos) < digits)
                    throwSyntaxError("invalid unicode escape sequence in quoted string");
                UInt32 code_point = 0;
                for (size_t i = 0; i < digits; ++i)
                {
                    int d = hexDigit(pos[i]);
                    if (d < 0)
                        throwSyntaxError("invalid unicode escape sequence in quoted string");
                    code_point = code_point * 16 + static_cast<UInt32>(d);
                }
                if (code_point > 0x10FFFF || (code_point >= 0xD800 && code_point <= 0xDFFF))
                    throwSyntaxError("invalid code point in unicode escape sequence");
                appendUTF8(result, code_point);
                pos += digits;
                break;
            }
            case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
            {
                if (end - pos < 3 || pos[1] < '0' || pos[1] > '7' || pos[2] < '0' || pos[2] > '7')
                    throwSyntaxError("invalid octal escape sequence in quoted string");
                int value = (pos[0] - '0') * 64 + (pos[1] - '0') * 8 + (pos[2] - '0');
                if (value > 255)
                    throwSyntaxError("invalid octal escape sequence in quoted string");
                result += static_cast<char>(value);
                pos += 3;
                break;
            }
            default:
            {
                if (e == quote)
                {
                    result += e;
                    ++pos;
                    break;
                }
                throwSyntaxError("invalid escape sequence in quoted string");
            }
        }
    }
}

String LogsQLLexer::decodeBacktickQuoted(const char *& pos) const
{
    /// Go raw string literal: no escape sequences, carriage returns are discarded.
    ++pos;
    String result;
    while (true)
    {
        if (pos == end)
            throwSyntaxError("unterminated backtick-quoted string");
        char c = *pos;
        ++pos;
        if (c == '`')
            return result;
        if (c != '\r')
            result += c;
    }
}

String LogsQLLexer::decodeSingleQuoted(const char *& pos) const
{
    return decodeDoubleQuoted(pos);
}

void LogsQLLexer::nextToken()
{
    prev_raw_token = String(raw_token);
    token.clear();
    raw_token = {};
    quoted = false;
    skipped_space = false;

    const char * s = current;

    while (true)
    {
        if (s == end)
        {
            token_begin = end;
            current = end;
            return;
        }

        if (isWhitespaceChar(*s))
        {
            skipped_space = true;
            ++s;
            continue;
        }

        if (*s == '#')
        {
            /// Comment spans to the end of the line.
            while (s != end && *s != '\n')
                ++s;
            skipped_space = true;
            continue;
        }

        break;
    }

    token_begin = s;

    /// A run of word characters.
    if (isWordChar(*s))
    {
        const char * word_end = s;
        while (word_end != end && isWordChar(*word_end))
            ++word_end;
        raw_token = std::string_view(s, word_end);
        token = String(raw_token);
        current = word_end;
        return;
    }

    switch (*s)
    {
        case '"':
        {
            const char * pos = s;
            token = decodeDoubleQuoted(pos);
            raw_token = std::string_view(s, pos);
            quoted = true;
            current = pos;
            return;
        }
        case '`':
        {
            const char * pos = s;
            token = decodeBacktickQuoted(pos);
            raw_token = std::string_view(s, pos);
            quoted = true;
            current = pos;
            return;
        }
        case '\'':
        {
            const char * pos = s;
            token = decodeSingleQuoted(pos);
            raw_token = std::string_view(s, pos);
            quoted = true;
            current = pos;
            return;
        }
        case '=':
        {
            size_t size = (s + 1 != end && s[1] == '~') ? 2 : 1;
            raw_token = std::string_view(s, size);
            token = String(raw_token);
            current = s + size;
            return;
        }
        case '!':
        {
            size_t size = (s + 1 != end && (s[1] == '~' || s[1] == '=')) ? 2 : 1;
            raw_token = std::string_view(s, size);
            token = String(raw_token);
            current = s + size;
            return;
        }
        default:
        {
            raw_token = std::string_view(s, 1);
            token = String(raw_token);
            current = s + 1;
            return;
        }
    }
}

bool LogsQLLexer::isAllowedCompoundToken(const std::vector<std::string_view> & stop_tokens) const
{
    if (quoted || token.empty())
        return false;

    for (const auto & stop : stop_tokens)
        if (isKeyword(stop))
            return false;

    for (const auto & glue : glue_compound_tokens)
        if (token == glue)
            return true;

    return isWord(token);
}

String LogsQLLexer::nextCompoundToken(const std::vector<std::string_view> & stop_tokens)
{
    if (quoted)
    {
        String result = token;
        nextToken();
        return result;
    }

    if (!skipped_space && isWord(prev_raw_token))
    {
        for (const auto & denied : denied_first_compound_tokens)
        {
            if (token == denied)
                throwSyntaxError(fmt::format("missing whitespace between {} and {}", prev_raw_token, token));
        }
    }

    if (!isAllowedCompoundToken(stop_tokens))
        throwSyntaxError(fmt::format("compound token cannot start with {}; put it into quotes if needed",
            token.empty() ? String("end of query") : token));

    String result = token;
    nextToken();

    while (!skipped_space && isAllowedCompoundToken(stop_tokens))
    {
        result += String(raw_token);
        nextToken();
    }

    /// Disallow a single-character compound token consisting of a glue character - this is error-prone.
    if (result.size() == 1)
    {
        for (const auto & glue : glue_compound_tokens)
            if (result == glue)
                throwSyntaxError(fmt::format("compound token cannot be equal to {}; put it into quotes if needed", result));
    }

    return result;
}

}
