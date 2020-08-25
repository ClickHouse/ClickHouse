#include <Parsers/Lexer.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/find_symbols.h>


namespace DB
{

namespace
{

/// This must be consistent with functions in ReadHelpers.h
template <char quote, TokenType success_token, TokenType error_token>
Token quotedString(const char *& pos, const char * const token_begin, const char * const end)
{
    ++pos;
    while (true)
    {
        pos = find_first_symbols<quote, '\\'>(pos, end);
        if (pos >= end)
            return Token(error_token, token_begin, end);

        if (*pos == quote)
        {
            ++pos;
            if (pos < end && *pos == quote)
            {
                ++pos;
                continue;
            }
            return Token(success_token, token_begin, pos);
        }

        if (*pos == '\\')
        {
            ++pos;
            if (pos >= end)
                return Token(error_token, token_begin, end);
            ++pos;
            continue;
        }

        __builtin_unreachable();
    }
}

}


Token Lexer::nextToken()
{
    Token res = nextTokenImpl();
    if (res.type != TokenType::EndOfStream && max_query_size && res.end > begin + max_query_size)
        res.type = TokenType::ErrorMaxQuerySizeExceeded;
    if (res.isSignificant())
        prev_significant_token_type = res.type;
    return res;
}


Token Lexer::nextTokenImpl()
{
    if (pos >= end)
        return Token(TokenType::EndOfStream, end, end);

    const char * const token_begin = pos;

    auto comment_until_end_of_line = [&]() mutable
    {
        pos = find_first_symbols<'\n'>(pos, end);    /// This means that newline in single-line comment cannot be escaped.
        return Token(TokenType::Comment, token_begin, pos);
    };

    switch (*pos)
    {
        case ' ': [[fallthrough]];
        case '\t': [[fallthrough]];
        case '\n': [[fallthrough]];
        case '\r': [[fallthrough]];
        case '\f': [[fallthrough]];
        case '\v':
        {
            ++pos;
            while (pos < end && isWhitespaceASCII(*pos))
                ++pos;
            return Token(TokenType::Whitespace, token_begin, pos);
        }

        case '0': [[fallthrough]];
        case '1': [[fallthrough]];
        case '2': [[fallthrough]];
        case '3': [[fallthrough]];
        case '4': [[fallthrough]];
        case '5': [[fallthrough]];
        case '6': [[fallthrough]];
        case '7': [[fallthrough]];
        case '8': [[fallthrough]];
        case '9':
        {
            /// The task is not to parse a number or check correctness, but only to skip it.

            /// Disambiguation: if previous token was dot, then we could parse only simple integer,
            ///  for chained tuple access operators (x.1.1) to work.
            //  Otherwise it will be tokenized as x . 1.1, not as x . 1 . 1
            if (prev_significant_token_type == TokenType::Dot)
            {
                ++pos;
                while (pos < end && isNumericASCII(*pos))
                    ++pos;
            }
            else
            {
                /// 0x, 0b
                bool hex = false;
                if (pos + 2 < end && *pos == '0' && (pos[1] == 'x' || pos[1] == 'b' || pos[1] == 'X' || pos[1] == 'B'))
                {
                    if (pos[1] == 'x' || pos[1] == 'X')
                        hex = true;
                    pos += 2;
                }
                else
                    ++pos;

                while (pos < end && (hex ? isHexDigit(*pos) : isNumericASCII(*pos)))
                    ++pos;

                /// decimal point
                if (pos < end && *pos == '.')
                {
                    ++pos;
                    while (pos < end && (hex ? isHexDigit(*pos) : isNumericASCII(*pos)))
                        ++pos;
                }

                /// exponentiation (base 10 or base 2)
                if (pos + 1 < end && (hex ? (*pos == 'p' || *pos == 'P') : (*pos == 'e' || *pos == 'E')))
                {
                    ++pos;

                    /// sign of exponent. It is always decimal.
                    if (pos + 1 < end && (*pos == '-' || *pos == '+'))
                        ++pos;

                    while (pos < end && isNumericASCII(*pos))
                        ++pos;
                }
            }

            /// word character cannot go just after number (SELECT 123FROM)
            if (pos < end && isWordCharASCII(*pos))
            {
                ++pos;
                while (pos < end && isWordCharASCII(*pos))
                    ++pos;
                return Token(TokenType::ErrorWrongNumber, token_begin, pos);
            }

            return Token(TokenType::Number, token_begin, pos);
        }

        case '\'':
            return quotedString<'\'', TokenType::StringLiteral, TokenType::ErrorSingleQuoteIsNotClosed>(pos, token_begin, end);
        case '"':
            return quotedString<'"', TokenType::QuotedIdentifier, TokenType::ErrorDoubleQuoteIsNotClosed>(pos, token_begin, end);
        case '`':
            return quotedString<'`', TokenType::QuotedIdentifier, TokenType::ErrorBackQuoteIsNotClosed>(pos, token_begin, end);

        case '(':
            return Token(TokenType::OpeningRoundBracket, token_begin, ++pos);
        case ')':
            return Token(TokenType::ClosingRoundBracket, token_begin, ++pos);
        case '[':
            return Token(TokenType::OpeningSquareBracket, token_begin, ++pos);
        case ']':
            return Token(TokenType::ClosingSquareBracket, token_begin, ++pos);
        case '{':
            return Token(TokenType::OpeningCurlyBrace, token_begin, ++pos);
        case '}':
            return Token(TokenType::ClosingCurlyBrace, token_begin, ++pos);
        case ',':
            return Token(TokenType::Comma, token_begin, ++pos);
        case ';':
            return Token(TokenType::Semicolon, token_begin, ++pos);

        case '.':   /// qualifier, tuple access operator or start of floating point number
        {
            /// Just after identifier or complex expression or number (for chained tuple access like x.1.1 to work properly).
            if (pos > begin
                && (!(pos + 1 < end && isNumericASCII(pos[1]))
                    || prev_significant_token_type == TokenType::ClosingRoundBracket
                    || prev_significant_token_type == TokenType::ClosingSquareBracket
                    || prev_significant_token_type == TokenType::BareWord
                    || prev_significant_token_type == TokenType::QuotedIdentifier
                    || prev_significant_token_type == TokenType::Number))
                return Token(TokenType::Dot, token_begin, ++pos);

            ++pos;
            while (pos < end && isNumericASCII(*pos))
                ++pos;

            /// exponentiation
            if (pos + 1 < end && (*pos == 'e' || *pos == 'E'))
            {
                ++pos;

                /// sign of exponent
                if (pos + 1 < end && (*pos == '-' || *pos == '+'))
                    ++pos;

                while (pos < end && isNumericASCII(*pos))
                    ++pos;
            }

            return Token(TokenType::Number, token_begin, pos);
        }

        case '+':
            return Token(TokenType::Plus, token_begin, ++pos);
        case '-':   /// minus (-), arrow (->) or start of comment (--)
        {
            ++pos;
            if (pos < end && *pos == '>')
                return Token(TokenType::Arrow, token_begin, ++pos);

            if (pos < end && *pos == '-')
            {
                ++pos;
                return comment_until_end_of_line();
            }

            return Token(TokenType::Minus, token_begin, pos);
        }
        case '*':
            ++pos;
            return Token(TokenType::Asterisk, token_begin, pos);
        case '/':   /// division (/) or start of comment (//, /*)
        {
            ++pos;
            if (pos < end && (*pos == '/' || *pos == '*'))
            {
                if (*pos == '/')
                {
                    ++pos;
                    return comment_until_end_of_line();
                }
                else
                {
                    ++pos;
                    while (pos + 2 <= end)
                    {
                        /// This means that nested multiline comments are not supported.
                        if (pos[0] == '*' && pos[1] == '/')
                        {
                            pos += 2;
                            return Token(TokenType::Comment, token_begin, pos);
                        }
                        ++pos;
                    }
                    return Token(TokenType::ErrorMultilineCommentIsNotClosed, token_begin, end);
                }
            }
            return Token(TokenType::Slash, token_begin, pos);
        }
        case '%':
            return Token(TokenType::Percent, token_begin, ++pos);
        case '=':   /// =, ==
        {
            ++pos;
            if (pos < end && *pos == '=')
                ++pos;
            return Token(TokenType::Equals, token_begin, pos);
        }
        case '!':   /// !=
        {
            ++pos;
            if (pos < end && *pos == '=')
                return Token(TokenType::NotEquals, token_begin, ++pos);
            return Token(TokenType::ErrorSingleExclamationMark, token_begin, pos);
        }
        case '<':   /// <, <=, <>
        {
            ++pos;
            if (pos < end && *pos == '=')
                return Token(TokenType::LessOrEquals, token_begin, ++pos);
            if (pos < end && *pos == '>')
                return Token(TokenType::NotEquals, token_begin, ++pos);
            return Token(TokenType::Less, token_begin, pos);
        }
        case '>':   /// >, >=
        {
            ++pos;
            if (pos < end && *pos == '=')
                return Token(TokenType::GreaterOrEquals, token_begin, ++pos);
            return Token(TokenType::Greater, token_begin, pos);
        }
        case '?':
            return Token(TokenType::QuestionMark, token_begin, ++pos);
        case ':':
            return Token(TokenType::Colon, token_begin, ++pos);
        case '|':
        {
            ++pos;
            if (pos < end && *pos == '|')
                return Token(TokenType::Concatenation, token_begin, ++pos);
            return Token(TokenType::ErrorSinglePipeMark, token_begin, pos);
        }
        case '@':
        {
            ++pos;
            if (pos < end && *pos == '@')
                return Token(TokenType::DoubleAt, token_begin, ++pos);
            return Token(TokenType::At, token_begin, pos);
        }

        default:
            if (isWordCharASCII(*pos))
            {
                ++pos;
                while (pos < end && isWordCharASCII(*pos))
                    ++pos;
                return Token(TokenType::BareWord, token_begin, pos);
            }
            else
            {
                /// We will also skip unicode whitespaces in UTF-8 to support for queries copy-pasted from MS Word and similar.
                pos = skipWhitespacesUTF8(pos, end);
                if (pos > token_begin)
                    return Token(TokenType::Whitespace, token_begin, pos);
                else
                    return Token(TokenType::Error, token_begin, ++pos);
            }
    }
}


const char * getTokenName(TokenType type)
{
    switch (type)
    {
#define M(TOKEN) \
        case TokenType::TOKEN: return #TOKEN;
APPLY_FOR_TOKENS(M)
#undef M
    }

    __builtin_unreachable();
}


const char * getErrorTokenDescription(TokenType type)
{
    switch (type)
    {
        case TokenType::Error:
            return "Unrecognized token";
        case TokenType::ErrorMultilineCommentIsNotClosed:
            return "Multiline comment is not closed";
        case TokenType::ErrorSingleQuoteIsNotClosed:
            return "Single quoted string is not closed";
        case TokenType::ErrorDoubleQuoteIsNotClosed:
            return "Double quoted string is not closed";
        case TokenType::ErrorBackQuoteIsNotClosed:
            return "Back quoted string is not closed";
        case TokenType::ErrorSingleExclamationMark:
            return "Exclamation mark can only occur in != operator";
        case TokenType::ErrorSinglePipeMark:
            return "Pipe symbol could only occur in || operator";
        case TokenType::ErrorWrongNumber:
            return "Wrong number";
        case TokenType::ErrorMaxQuerySizeExceeded:
            return "Max query size exceeded";
        default:
            return "Not an error";
    }
}

}
