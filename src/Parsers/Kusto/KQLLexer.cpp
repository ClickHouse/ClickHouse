#include <cassert>
#include <base/defines.h>
#include <Parsers/Kusto/KQLLexer.h>
#include <Common/StringUtils.h>
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

        case '-':   /// minus (-), arrow (->) or start of comment (--)
        {
            ++pos;
            if (pos < end && *pos == '-')
            {
                ++pos;
                return comment_until_end_of_line();
            }

            return KQLToken(KQLTokenType::Minus, token_begin, pos);
        }
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
            return KQLToken(KQLTokenType::Error, token_begin, pos);
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
            return KQLToken(KQLTokenType::ExclamationMark, token_begin, pos);
        }
        case '|':
        {
            ++pos;
            return KQLToken(KQLTokenType::PipeMark, token_begin, pos);
        }
        case '~':
            return KQLToken(KQLTokenType::Tilde, token_begin, ++pos);
        default:
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
