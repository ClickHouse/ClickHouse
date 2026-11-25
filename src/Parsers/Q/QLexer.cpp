#include <Parsers/Q/QLexer.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

bool isVerbChar(char c)
{
    switch (c)
    {
        case '+': case '-': case '*': case '%': case '!': case '&': case '|': case '<': case '>':
        case '=': case '~': case ',': case '^': case '#': case '_': case '$': case '?': case '@': case '.':
            return true;
        default:
            return false;
    }
}

bool isNameStart(char c)
{
    return isAlphaASCII(c);
}

bool isNameChar(char c)
{
    return isAlphaNumericASCII(c) || c == '_';
}

bool isSymbolChar(char c)
{
    return isAlphaNumericASCII(c) || c == '_' || c == '.';
}

/// Whether a token of this type ends a noun, so that a following `-1` is a subtraction rather than a negative literal.
bool endsNoun(QTokenType type)
{
    switch (type)
    {
        case QTokenType::Number:
        case QTokenType::Symbol:
        case QTokenType::String:
        case QTokenType::Name:
        case QTokenType::CloseParen:
        case QTokenType::CloseBracket:
        case QTokenType::CloseBrace:
            return true;
        default:
            return false;
    }
}

class QLexer
{
public:
    explicit QLexer(std::string_view text_) : text(text_) {}

    QTokens tokenize()
    {
        while (true)
        {
            bool space = skipWhitespaceAndComments();
            if (pos >= text.size())
                break;

            char c = text[pos];
            if (c == '\n')
            {
                handleNewline();
                continue;
            }

            size_t begin = pos;
            QToken token;
            token.offset = begin;
            token.space_before = space || tokens.empty();

            if (isNumberStart(token.space_before))
                scanNumber(token);
            else if (c == '`')
                scanSymbol(token);
            else if (c == '"')
                scanString(token);
            else if (isNameStart(c) || (c == '.' && pos + 1 < text.size() && isNameStart(text[pos + 1])))
                scanName(token);
            else if (c == '\'')
            {
                ++pos;
                if (pos < text.size() && text[pos] == ':')
                    ++pos;
                token.type = QTokenType::Adverb;
            }
            else if (c == '/' || c == '\\')
            {
                ++pos;
                if (pos < text.size() && text[pos] == ':')
                    ++pos;
                token.type = QTokenType::Adverb;
            }
            else if (c == ':')
            {
                ++pos;
                if (pos < text.size() && text[pos] == ':')
                {
                    ++pos;
                    token.type = QTokenType::DoubleColon;
                }
                else
                    token.type = QTokenType::Colon;
            }
            else if (isVerbChar(c))
            {
                ++pos;
                if ((c == '<' || c == '>') && pos < text.size() && (text[pos] == '=' || (c == '<' && text[pos] == '>')))
                    ++pos;
                if (pos < text.size() && text[pos] == ':' && !(pos + 1 < text.size() && text[pos + 1] == ':'))
                    ++pos;
                token.type = QTokenType::Verb;
            }
            else
            {
                switch (c)
                {
                    case '(': token.type = QTokenType::OpenParen; ++depth; break;
                    case ')': token.type = QTokenType::CloseParen; --depth; break;
                    case '[': token.type = QTokenType::OpenBracket; ++depth; break;
                    case ']': token.type = QTokenType::CloseBracket; --depth; break;
                    case '{': token.type = QTokenType::OpenBrace; ++depth; break;
                    case '}': token.type = QTokenType::CloseBrace; --depth; break;
                    case ';': token.type = QTokenType::Semicolon; break;
                    default:
                        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected character '{}' at position {} of the q program", c, pos + 1);
                }
                ++pos;
            }

            token.text = text.substr(begin, pos - begin);
            tokens.push_back(std::move(token));
        }

        QToken end;
        end.type = QTokenType::End;
        end.offset = text.size();
        end.space_before = true;
        tokens.push_back(std::move(end));
        return std::move(tokens);
    }

private:
    std::string_view text;
    size_t pos = 0;
    int depth = 0;
    QTokens tokens;

    /// The text before this position is known to hold only blank and comment-only lines up to a continuation of
    /// the statement, see `handleNewline`.
    size_t continuation_until = 0;

    [[nodiscard]] bool atLineStart(size_t at) const
    {
        return at == 0 || text[at - 1] == '\n';
    }

    /// The text of the line starting at `at`, without the trailing whitespace.
    [[nodiscard]] std::string_view lineAt(size_t at) const
    {
        size_t end = text.find('\n', at);
        if (end == std::string_view::npos)
            end = text.size();
        std::string_view line = text.substr(at, end - at);
        while (!line.empty() && isWhitespaceASCII(line.back()))
            line.remove_suffix(1);
        return line;
    }

    /// Skips spaces, tabs, and comments, but not line breaks. Returns whether anything was skipped
    /// (a comment counts as whitespace).
    bool skipWhitespaceAndComments()
    {
        bool skipped = atLineStart(pos);
        while (pos < text.size())
        {
            char c = text[pos];
            if (c == ' ' || c == '\t' || c == '\r')
            {
                ++pos;
                skipped = true;
                continue;
            }

            /// A line starting with `\` is a system command, unless it is a lone `\`, which ends the script.
            if (c == '\\' && atLineStart(pos) && lineAt(pos) != "\\")
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "System commands (lines starting with '\\') are not supported in the q dialect");

            const char * data = text.data();
            const char * after = skipQComment(data, data + pos, data + text.size());
            if (after != data + pos)
            {
                pos = static_cast<size_t>(after - data);
                skipped = true;
                continue;
            }
            break;
        }
        return skipped;
    }

    void handleNewline()
    {
        ++pos;
        /// A line break ends a statement when it is not inside brackets and the next non-empty line
        /// starts in the first column (a line starting with whitespace continues the statement).
        if (depth > 0 || tokens.empty() || tokens.back().type == QTokenType::StatementBreak)
            return;

        /// The lookahead from a previous line break of the same run of blank and comment-only lines has already found
        /// that the statement continues after them. Without this, every line break of the run would scan the rest of it:
        /// quadratic work for an expression with a hundred thousand blank lines inside.
        if (pos <= continuation_until)
            return;

        /// Lines with nothing but whitespace and comments (of every kind that the dialect accepts) do not count.
        const char * data = text.data();
        const char * end = data + text.size();
        const char * next = data + pos;
        while (next < end)
        {
            if (isWhitespaceASCII(*next))
            {
                ++next;
                continue;
            }
            const char * after = skipQComment(data, next, end);
            if (after == next)
                break;
            next = after;
        }
        if (next >= end || !atLineStart(static_cast<size_t>(next - data)))
        {
            continuation_until = static_cast<size_t>(next - data);
            return;
        }

        QToken token;
        token.type = QTokenType::StatementBreak;
        token.offset = pos - 1;
        token.text = text.substr(pos - 1, 1);
        tokens.push_back(std::move(token));
    }

    [[nodiscard]] bool isNumberStart(bool space_before) const
    {
        char c = text[pos];
        auto is_digit_at = [&](size_t at) { return at < text.size() && isNumericASCII(text[at]); };

        if (isNumericASCII(c))
            return true;
        if (c == '.' && is_digit_at(pos + 1))
            return tokens.empty() || space_before || !endsNoun(tokens.back().type) || tokens.back().type != QTokenType::Name;
        if (c == '-' && (is_digit_at(pos + 1) || (pos + 2 < text.size() && text[pos + 1] == '.' && is_digit_at(pos + 2))))
        {
            /// `x-1` is a subtraction, while `x -1` and `(-1)` contain a negative literal.
            return tokens.empty() || space_before || !endsNoun(tokens.back().type);
        }
        return false;
    }

    void scanNumber(QToken & token)
    {
        token.type = QTokenType::Number;
        size_t begin = pos;
        if (text[pos] == '-')
            ++pos;
        bool hex = text.substr(pos, 2) == "0x";
        while (pos < text.size())
        {
            char c = text[pos];
            if (isAlphaNumericASCII(c) || c == '.' || c == ':')
            {
                /// `10:30` is a minute, but `x:10` is an assignment, so a colon must be followed by a digit.
                if (c == ':' && !(pos + 1 < text.size() && isNumericASCII(text[pos + 1])))
                    break;
                ++pos;
            }
            else if ((c == '-' || c == '+') && !hex && pos > begin && (text[pos - 1] == 'e' || text[pos - 1] == 'E')
                && pos + 1 < text.size() && isNumericASCII(text[pos + 1]))
            {
                /// The exponent of a float: `1e-3`.
                ++pos;
            }
            else
                break;
        }
    }

    void scanSymbol(QToken & token)
    {
        token.type = QTokenType::Symbol;
        ++pos;
        size_t begin = pos;
        if (pos < text.size() && text[pos] == ':')
        {
            /// A file handle, like `` `:path/to/file ``.
            ++pos;
            while (pos < text.size() && (isSymbolChar(text[pos]) || text[pos] == '/' || text[pos] == ':'))
                ++pos;
        }
        else
        {
            while (pos < text.size() && isSymbolChar(text[pos]))
                ++pos;
        }
        token.value = String(text.substr(begin, pos - begin));
    }

    void scanString(QToken & token)
    {
        token.type = QTokenType::String;
        size_t begin = pos;
        ++pos;
        String value;
        while (true)
        {
            if (pos >= text.size())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unterminated string starting at position {} of the q program", begin + 1);
            char c = text[pos];
            if (c == '"')
            {
                ++pos;
                break;
            }
            if (c == '\\')
            {
                ++pos;
                if (pos >= text.size())
                    continue;
                char e = text[pos];
                switch (e)
                {
                    case 'n': value += '\n'; ++pos; break;
                    case 'r': value += '\r'; ++pos; break;
                    case 't': value += '\t'; ++pos; break;
                    case '\\': value += '\\'; ++pos; break;
                    case '"': value += '"'; ++pos; break;
                    default:
                        if (e >= '0' && e <= '7')
                        {
                            /// An octal escape of exactly three digits: `\012`.
                            unsigned code = 0;
                            size_t digits = 0;
                            while (digits < 3 && pos < text.size() && text[pos] >= '0' && text[pos] <= '7')
                            {
                                code = code * 8 + static_cast<unsigned>(text[pos] - '0');
                                ++pos;
                                ++digits;
                            }
                            if (digits != 3 || code > 255)
                                throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid octal escape in a string at position {} of the q program", pos);
                            value += static_cast<char>(code);
                        }
                        else
                            throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid escape sequence '\\{}' in a string at position {} of the q program", e, pos);
                }
                continue;
            }
            value += c;
            ++pos;
        }
        token.value = std::move(value);
    }

    void scanName(QToken & token)
    {
        token.type = QTokenType::Name;
        if (text[pos] == '.')
            ++pos;
        while (pos < text.size())
        {
            if (isNameChar(text[pos]))
                ++pos;
            else if (text[pos] == '.' && pos + 1 < text.size() && isNameStart(text[pos + 1]))
                ++pos;
            else
                break;
        }
    }
};

}

QTokens tokenizeQ(std::string_view text)
{
    return QLexer(text).tokenize();
}

const char * skipQComment(const char * begin, const char * at, const char * end)
{
    if (at >= end)
        return at;

    auto line_end = [&](const char * from)
    {
        while (from < end && *from != '\n')
            ++from;
        return from;
    };

    /// Whether the line starting at `from` consists of the character `c` alone (trailing whitespace is allowed).
    auto is_lone = [&](const char * from, char c)
    {
        if (from >= end || *from != c)
            return false;
        for (const char * rest = from + 1; rest < end && *rest != '\n'; ++rest)
            if (!isWhitespaceASCII(*rest))
                return false;
        return true;
    };

    bool line_start = at == begin || at[-1] == '\n';
    if (line_start)
    {
        if (is_lone(at, '/'))
        {
            /// A block comment: from a line with a lone `/` to a line with a lone `\`.
            const char * line = at;
            while (true)
            {
                line = line_end(line);
                if (line == end)
                    return end;
                ++line;
                if (is_lone(line, '\\'))
                    return line_end(line);
            }
        }
        /// A lone `\` ends the script: the rest is a comment.
        if (is_lone(at, '\\'))
            return end;
    }

    if (!line_start && !isWhitespaceASCII(at[-1]))
        return at;

    bool sql_comment = *at == '-' && at + 1 < end && at[1] == '-' && (at + 2 == end || isWhitespaceASCII(at[2]));
    if (*at == '/' || sql_comment)
        return line_end(at);
    return at;
}

}
