#include <cassert>
#include <base/defines.h>
#include <Parsers/Lexer.h>
#include <Common/StringUtils.h>
#include <base/find_symbols.h>

namespace DB
{

namespace
{

/// This must be consistent with functions in ReadHelpers.h
template <char quote>
Token quotedString(const char *& pos, const char * const token_begin, const char * const end,
    TokenType success_token, TokenType error_token)
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

        chassert(false);
    }
}

Token quotedStringWithUnicodeQuotes(const char *& pos, const char * const token_begin, const char * const end,
    char expected_end_byte, TokenType success_token, TokenType error_token)
{
    /// ‘: e2 80 98
    /// ’: e2 80 99
    /// “: e2 80 9c
    /// ”: e2 80 9d

    while (true)
    {
        pos = find_first_symbols<'\xE2'>(pos, end);
        if (pos + 2 >= end)
            return Token(error_token, token_begin, end);

        if (pos[0] == '\xE2' && pos[1] == '\x80' && pos[2] == expected_end_byte)
        {
            pos += 3;
            return Token(success_token, token_begin, pos);
        }

        ++pos;
    }
}

Token quotedHexOrBinString(const char *& pos, const char * const token_begin, const char * const end)
{
    constexpr char quote = '\'';

    assert(pos[1] == quote);

    bool hex = (*pos == 'x' || *pos == 'X');

    pos += 2;

    if (hex)
    {
        while (pos < end && isHexDigit(*pos))
            ++pos;
    }
    else
    {
        pos = find_first_not_symbols<'0', '1'>(pos, end);
    }

    if (pos >= end || *pos != quote)
    {
        pos = end;
        return Token(TokenType::ErrorSingleQuoteIsNotClosed, token_begin, end);
    }

    ++pos;
    return Token(TokenType::StringLiteral, token_begin, pos);
}

}


Token Lexer::nextToken()
{
    Token res = nextTokenImpl();
    if (max_query_size && res.end > begin + max_query_size)
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
        case ' ':
        case '\t':
        case '\n':
        case '\r':
        case '\f':
        case '\v':
        {
            ++pos;
            while (pos < end && isWhitespaceASCII(*pos))
                ++pos;
            return Token(TokenType::Whitespace, token_begin, pos);
        }

        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        {
            /// The task is not to parse a number or check correctness, but only to skip it.

            /// Disambiguation: if previous token was dot, then we could parse only simple integer,
            ///  for chained tuple access operators (x.1.1) to work.
            //  Otherwise it will be tokenized as x . 1.1, not as x . 1 . 1
            if (prev_significant_token_type == TokenType::Dot)
            {
                ++pos;
                while (pos < end && (isNumericASCII(*pos) || isNumberSeparator(false, false, pos, end)))
                    ++pos;
            }
            else
            {
                bool start_of_block = false;
                /// 0x, 0b
                bool hex = false;
                if (pos + 2 < end && *pos == '0' && (pos[1] == 'x' || pos[1] == 'b' || pos[1] == 'X' || pos[1] == 'B'))
                {
                    bool is_valid = false;
                    if (pos[1] == 'x' || pos[1] == 'X')
                    {
                        if (isHexDigit(pos[2]))
                        {
                            hex = true;
                            is_valid = true; // hex
                        }
                    }
                    else if (pos[2] == '0' || pos[2] == '1')
                        is_valid = true; // bin
                    if (is_valid)
                    {
                        pos += 2;
                        start_of_block = true;
                    }
                    else
                        ++pos; // consume the leading zero - could be an identifier
                }
                else
                    ++pos;

                while (pos < end && ((hex ? isHexDigit(*pos) : isNumericASCII(*pos)) || isNumberSeparator(start_of_block, hex, pos, end)))
                {
                    ++pos;
                    start_of_block = false;
                }

                /// decimal point
                if (pos < end && *pos == '.')
                {
                    start_of_block = true;
                    ++pos;
                    while (pos < end && ((hex ? isHexDigit(*pos) : isNumericASCII(*pos)) || isNumberSeparator(start_of_block, hex, pos, end)))
                    {
                        ++pos;
                        start_of_block = false;
                    }
                }

                /// exponentiation (base 10 or base 2)
                if (pos + 1 < end && (hex ? (*pos == 'p' || *pos == 'P') : (*pos == 'e' || *pos == 'E')))
                {
                    start_of_block = true;
                    ++pos;

                    /// sign of exponent. It is always decimal.
                    if (pos + 1 < end && (*pos == '-' || *pos == '+'))
                        ++pos;

                    while (pos < end && (isNumericASCII(*pos) || isNumberSeparator(start_of_block, false, pos, end)))
                    {
                        ++pos;
                        start_of_block = false;
                    }
                }
            }

            /// Try to parse it to a identifier(1identifier_name), otherwise it return ErrorWrongNumber
            if (pos < end && isWordCharASCII(*pos))
            {
                ++pos;
                while (pos < end && isWordCharASCII(*pos))
                    ++pos;

                for (const char * iterator = token_begin; iterator < pos; ++iterator)
                {
                    if (!isWordCharASCII(*iterator) && *iterator != '$')
                        return Token(TokenType::ErrorWrongNumber, token_begin, pos);
                }

                return Token(TokenType::BareWord, token_begin, pos);
            }

            return Token(TokenType::Number, token_begin, pos);
        }

        case '\'':
            return quotedString<'\''>(pos, token_begin, end, TokenType::StringLiteral, TokenType::ErrorSingleQuoteIsNotClosed);
        case '"':
            return quotedString<'"'>(pos, token_begin, end, TokenType::QuotedIdentifier, TokenType::ErrorDoubleQuoteIsNotClosed);
        case '`':
            return quotedString<'`'>(pos, token_begin, end, TokenType::QuotedIdentifier, TokenType::ErrorBackQuoteIsNotClosed);

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

            bool start_of_block = true;
            ++pos;
            while (pos < end && (isNumericASCII(*pos) || isNumberSeparator(start_of_block, false, pos, end)))
            {
                ++pos;
                start_of_block = false;
            }

            /// exponentiation
            if (pos + 1 < end && (*pos == 'e' || *pos == 'E'))
            {
                start_of_block = true;
                ++pos;

                /// sign of exponent
                if (pos + 1 < end && (*pos == '-' || *pos == '+'))
                    ++pos;

                while (pos < end && (isNumericASCII(*pos) || isNumberSeparator(start_of_block, false, pos, end)))
                {
                    ++pos;
                    start_of_block = false;
                }
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

                ++pos;

                /// Nested multiline comments are supported according to the SQL standard.
                size_t nesting_level = 1;

                while (pos + 2 <= end)
                {
                    if (pos[0] == '/' && pos[1] == '*')
                    {
                        pos += 2;
                        ++nesting_level;
                    }
                    else if (pos[0] == '*' && pos[1] == '/')
                    {
                        pos += 2;
                        --nesting_level;

                        if (nesting_level == 0)
                            return Token(TokenType::Comment, token_begin, pos);
                    }
                    else
                        ++pos;
                }
                pos = end;
                return Token(TokenType::ErrorMultilineCommentIsNotClosed, token_begin, pos);
            }
            return Token(TokenType::Slash, token_begin, pos);
        }
        case '#':   /// start of single line comment, MySQL style
        {           /// PostgreSQL has some operators using '#' character.
                    /// For less ambiguity, we will recognize a comment only if # is followed by whitespace.
                    /// or #! as a special case for "shebang".
                    /// #hello - not a comment
                    /// # hello - a comment
                    /// #!/usr/bin/clickhouse-local --queries-file - a comment
            ++pos;
            if (pos < end && (*pos == ' ' || *pos == '!'))
                return comment_until_end_of_line();
            return Token(TokenType::Error, token_begin, pos);
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
        case '<':   /// <, <=, <>, <=>
        {
            ++pos;
            if (pos + 1 < end && *pos == '=' && *(pos + 1) == '>')
            {
                pos += 2;
                return Token(TokenType::Spaceship, token_begin, pos);
            }
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
        case '^':
            return Token(TokenType::Caret, token_begin, ++pos);
        case ':':
        {
            ++pos;
            if (pos < end && *pos == ':')
                return Token(TokenType::DoubleColon, token_begin, ++pos);
            return Token(TokenType::Colon, token_begin, pos);
        }
        case '|':
        {
            ++pos;
            if (pos < end && *pos == '|')
                return Token(TokenType::Concatenation, token_begin, ++pos);
            return Token(TokenType::PipeMark, token_begin, pos);
        }
        case '@':
        {
            ++pos;
            if (pos < end && *pos == '@')
                return Token(TokenType::DoubleAt, token_begin, ++pos);
            return Token(TokenType::At, token_begin, pos);
        }
        case '\\':
        {
            ++pos;
            if (pos < end && *pos == 'G')
                return Token(TokenType::VerticalDelimiter, token_begin, ++pos);
            return Token(TokenType::Error, token_begin, pos);
        }
        case '\xE2':
        {
            /// Mathematical minus symbol, UTF-8
            if (pos + 3 <= end && pos[1] == '\x88' && pos[2] == '\x92')
            {
                pos += 3;
                return Token(TokenType::Minus, token_begin, pos);
            }
            /// Unicode quoted string, ‘Hello’ or “World”.
            if (pos + 5 < end && pos[0] == '\xE2' && pos[1] == '\x80' && (pos[2] == '\x98' || pos[2] == '\x9C'))
            {
                const char expected_end_byte = pos[2] + 1;
                TokenType success_token = pos[2] == '\x98' ? TokenType::StringLiteral : TokenType::QuotedIdentifier;
                TokenType error_token = pos[2] == '\x98' ? TokenType::ErrorSingleQuoteIsNotClosed : TokenType::ErrorDoubleQuoteIsNotClosed;
                pos += 3;
                return quotedStringWithUnicodeQuotes(pos, token_begin, end, expected_end_byte, success_token, error_token);
            }
            /// Other characters starting at E2 can be parsed, see skipWhitespacesUTF8
            [[fallthrough]];
        }
        default:
            if (*pos == '$')
            {
                /// Try to capture dollar sign as start of here doc

                std::string_view token_stream(pos, end - pos);
                auto heredoc_name_end_position = token_stream.find('$', 1);
                if (heredoc_name_end_position != std::string::npos)
                {
                    size_t heredoc_size = heredoc_name_end_position + 1;
                    std::string_view heredoc = {token_stream.data(), heredoc_size}; // NOLINT

                    size_t heredoc_end_position = token_stream.find(heredoc, heredoc_size);
                    if (heredoc_end_position != std::string::npos)
                    {

                        pos += heredoc_end_position;
                        pos += heredoc_size;

                        return Token(TokenType::HereDoc, token_begin, pos);
                    }
                }

                if (((pos + 1 < end && !isWordCharASCII(pos[1])) || pos + 1 == end))
                {
                    /// Capture standalone dollar sign
                    return Token(TokenType::DollarSign, token_begin, ++pos);
                }
            }

            if (pos + 2 < end && pos[1] == '\'' && (*pos == 'x' || *pos == 'b' || *pos == 'X' || *pos == 'B'))
            {
                return quotedHexOrBinString(pos, token_begin, end);
            }

            if (isWordCharASCII(*pos) || *pos == '$')
            {
                ++pos;
                while (pos < end && (isWordCharASCII(*pos) || *pos == '$'))
                    ++pos;
                return Token(TokenType::BareWord, token_begin, pos);
            }

            /// We will also skip unicode whitespaces in UTF-8 to support for queries copy-pasted from MS Word and similar.
            pos = skipWhitespacesUTF8(pos, end);
            if (pos > token_begin)
                return Token(TokenType::Whitespace, token_begin, pos);
            return Token(TokenType::Error, token_begin, ++pos);
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
