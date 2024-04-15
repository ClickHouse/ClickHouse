#include <cassert>
#include <base/defines.h>
#include <Parsers/Kusto/KQLLexer.h>
#include <Common/StringUtils/StringUtils.h>
#include <base/find_symbols.h>

namespace DB
{

namespace
{

/// This must be consistent with functions in ReadHelpers.h
template <char quote, KQLTokenType success_token, KQLTokenType error_token>
KQLToken quotedString(const char *& pos, const char * const token_begin, const char * const end)
{
    ++pos;
    while (true)
    {
        pos = find_first_symbols<quote, '\\'>(pos, end);
        if (pos >= end)
            return KQLToken(error_token, token_begin, end);

        if (*pos == quote)
        {
            ++pos;
            if (pos < end && *pos == quote)
            {
                ++pos;
                continue;
            }
            return KQLToken(success_token, token_begin, pos);
        }

        if (*pos == '\\')
        {
            ++pos;
            if (pos >= end)
                return KQLToken(error_token, token_begin, end);
            ++pos;
            continue;
        }

        UNREACHABLE();
    }
}

KQLToken quotedHexOrBinString(const char *& pos, const char * const token_begin, const char * const end)
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
        return KQLToken(KQLTokenType::ErrorSingleQuoteIsNotClosed, token_begin, end);
    }

    ++pos;
    return KQLToken(KQLTokenType::StringLiteral, token_begin, pos);
}

}


KQLToken KQLLexer::nextToken()
{
    KQLToken res = nextTokenImpl();
    if (max_query_size && res.end > begin + max_query_size)
        res.type = KQLTokenType::ErrorMaxQuerySizeExceeded;
    if (res.isSignificant())
        prev_significant_token_type = res.type;
    return res;
}


KQLToken KQLLexer::nextTokenImpl()
{
    if (pos >= end)
        return KQLToken(KQLTokenType::EndOfStream, end, end);

    const char * const token_begin = pos;

    auto comment_until_end_of_line = [&]() mutable
    {
        pos = find_first_symbols<'\n'>(pos, end);    /// This means that newline in single-line comment cannot be escaped.
        return KQLToken(KQLTokenType::Comment, token_begin, pos);
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
            return KQLToken(KQLTokenType::Whitespace, token_begin, pos);
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
            if (prev_significant_token_type == KQLTokenType::Dot)
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

                return KQLToken(KQLTokenType::BareWord, token_begin, pos);
            }

            return KQLToken(KQLTokenType::Number, token_begin, pos);
        }

        case '\'':
            return quotedString<'\'', KQLTokenType::StringLiteral, KQLTokenType::ErrorSingleQuoteIsNotClosed>(pos, token_begin, end);
        case '"':
            return quotedString<'"', KQLTokenType::QuotedIdentifier, KQLTokenType::ErrorDoubleQuoteIsNotClosed>(pos, token_begin, end);
        case '`':
            return quotedString<'`', KQLTokenType::QuotedIdentifier, KQLTokenType::ErrorBackQuoteIsNotClosed>(pos, token_begin, end);

        case '(':
            return KQLToken(KQLTokenType::OpeningRoundBracket, token_begin, ++pos);
        case ')':
            return KQLToken(KQLTokenType::ClosingRoundBracket, token_begin, ++pos);
        case '[':
            return KQLToken(KQLTokenType::OpeningSquareBracket, token_begin, ++pos);
        case ']':
            return KQLToken(KQLTokenType::ClosingSquareBracket, token_begin, ++pos);
        case '{':
            return KQLToken(KQLTokenType::OpeningCurlyBrace, token_begin, ++pos);
        case '}':
            return KQLToken(KQLTokenType::ClosingCurlyBrace, token_begin, ++pos);
        case ',':
            return KQLToken(KQLTokenType::Comma, token_begin, ++pos);
        case ';':
            return KQLToken(KQLTokenType::Semicolon, token_begin, ++pos);

        case '.':   /// qualifier, tuple access operator or start of floating point number
        {
            /// Just after identifier or complex expression or number (for chained tuple access like x.1.1 to work properly).
            if (pos > begin
                && (!(pos + 1 < end && isNumericASCII(pos[1]))
                    || prev_significant_token_type == KQLTokenType::ClosingRoundBracket
                    || prev_significant_token_type == KQLTokenType::ClosingSquareBracket
                    || prev_significant_token_type == KQLTokenType::BareWord
                    || prev_significant_token_type == KQLTokenType::QuotedIdentifier
                    || prev_significant_token_type == KQLTokenType::Number))
                return KQLToken(KQLTokenType::Dot, token_begin, ++pos);

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

            return KQLToken(KQLTokenType::Number, token_begin, pos);
        }

        case '+':
            return KQLToken(KQLTokenType::Plus, token_begin, ++pos);
        case '-':   /// minus (-), arrow (->) or start of comment (--)
        {
            ++pos;
            if (pos < end && *pos == '>')
                return KQLToken(KQLTokenType::Arrow, token_begin, ++pos);

            if (pos < end && *pos == '-')
            {
                ++pos;
                return comment_until_end_of_line();
            }

            return KQLToken(KQLTokenType::Minus, token_begin, pos);
        }
        case '*':
            ++pos;
            return KQLToken(KQLTokenType::Asterisk, token_begin, pos);
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
                                return KQLToken(KQLTokenType::Comment, token_begin, pos);
                        }
                        else
                            ++pos;
                    }
                    pos = end;
                    return KQLToken(KQLTokenType::ErrorMultilineCommentIsNotClosed, token_begin, pos);
                }
            }
            return KQLToken(KQLTokenType::Slash, token_begin, pos);
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
            return KQLToken(KQLTokenType::Error, token_begin, pos);
        }
        case '%':
            return KQLToken(KQLTokenType::Percent, token_begin, ++pos);
        case '=':   /// =, ==
        {
            ++pos;
            if (pos < end && *pos == '=')
                return KQLToken(KQLTokenType::Equals, token_begin, ++pos);
            return KQLToken(KQLTokenType::Equals, token_begin, pos);
        }
        case '!':   /// !=
        {
            ++pos;
            if (pos < end && *pos == '=')
                return KQLToken(KQLTokenType::NotEquals, token_begin, ++pos);
            return KQLToken(KQLTokenType::ExclamationMark, token_begin, pos);
        }
        case '<':   /// <, <=, <>, <=>
        {
            ++pos;
            if (pos + 1 < end && *pos == '=' && *(pos + 1) == '>')
            {
                pos += 2;
                return KQLToken(KQLTokenType::Spaceship, token_begin, pos);
            }
            if (pos < end && *pos == '=')
                return KQLToken(KQLTokenType::LessOrEquals, token_begin, ++pos);
            if (pos < end && *pos == '>')
                return KQLToken(KQLTokenType::NotEquals, token_begin, ++pos);
            return KQLToken(KQLTokenType::Less, token_begin, pos);
        }
        case '>':   /// >, >=
        {
            ++pos;
            if (pos < end && *pos == '=')
                return KQLToken(KQLTokenType::GreaterOrEquals, token_begin, ++pos);
            return KQLToken(KQLTokenType::Greater, token_begin, pos);
        }
        case '?':
            return KQLToken(KQLTokenType::QuestionMark, token_begin, ++pos);
        case ':':
        {
            ++pos;
            if (pos < end && *pos == ':')
                return KQLToken(KQLTokenType::DoubleColon, token_begin, ++pos);
            return KQLToken(KQLTokenType::Colon, token_begin, pos);
        }
        case '|':
        {
            ++pos;
            if (pos < end && *pos == '|')
                return KQLToken(KQLTokenType::Concatenation, token_begin, ++pos);
            return KQLToken(KQLTokenType::PipeMark, token_begin, pos);
        }
        case '@':
        {
            ++pos;
            if (pos < end && *pos == '@')
                return KQLToken(KQLTokenType::DoubleAt, token_begin, ++pos);
            return KQLToken(KQLTokenType::At, token_begin, pos);
        }
        case '\\':
        {
            ++pos;
            if (pos < end && *pos == 'G')
                return KQLToken(KQLTokenType::VerticalDelimiter, token_begin, ++pos);
            return KQLToken(KQLTokenType::Error, token_begin, pos);
        }
        case '~':
            return KQLToken(KQLTokenType::Tilde, token_begin, ++pos);
        case '\xE2':
        {
            /// Mathematical minus symbol, UTF-8
            if (pos + 3 <= end && pos[1] == '\x88' && pos[2] == '\x92')
            {
                pos += 3;
                return KQLToken(KQLTokenType::Minus, token_begin, pos);
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
                    std::string_view heredoc = {token_stream.data(), heredoc_size};

                    size_t heredoc_end_position = token_stream.find(heredoc, heredoc_size);
                    if (heredoc_end_position != std::string::npos)
                    {

                        pos += heredoc_end_position;
                        pos += heredoc_size;

                        return KQLToken(KQLTokenType::HereDoc, token_begin, pos);
                    }
                }

                if (((pos + 1 < end && !isWordCharASCII(pos[1])) || pos + 1 == end))
                {
                    /// Capture standalone dollar sign
                    return KQLToken(KQLTokenType::DollarSign, token_begin, ++pos);
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
                return KQLToken(KQLTokenType::BareWord, token_begin, pos);
            }
            else
            {
                /// We will also skip unicode whitespaces in UTF-8 to support for queries copy-pasted from MS Word and similar.
                pos = skipWhitespacesUTF8(pos, end);
                if (pos > token_begin)
                    return KQLToken(KQLTokenType::Whitespace, token_begin, pos);
                else
                    return KQLToken(KQLTokenType::Error, token_begin, ++pos);
            }
    }
}


const char * getTokenName(KQLTokenType type)
{
    switch (type)
    {
#define M(TOKEN) \
        case KQLTokenType::TOKEN: return #TOKEN;
APPLY_FOR_KQLTOKENS(M)
#undef M
    }

    UNREACHABLE();
}


const char * getErrorTokenDescription(KQLTokenType type)
{
    switch (type)
    {
        case KQLTokenType::Error:
            return "Unrecognized token";
        case KQLTokenType::ErrorMultilineCommentIsNotClosed:
            return "Multiline comment is not closed";
        case KQLTokenType::ErrorSingleQuoteIsNotClosed:
            return "Single quoted string is not closed";
        case KQLTokenType::ErrorDoubleQuoteIsNotClosed:
            return "Double quoted string is not closed";
        case KQLTokenType::ErrorBackQuoteIsNotClosed:
            return "Back quoted string is not closed";
        case KQLTokenType::ErrorMaxQuerySizeExceeded:
            return "Max query size exceeded";
        default:
            return "Not an error";
    }
}

}
