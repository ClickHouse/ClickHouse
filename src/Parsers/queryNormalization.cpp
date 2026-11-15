#include <Parsers/Lexer.h>
#include <Parsers/queryNormalization.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>


namespace DB
{

UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names)
{
    SipHash hash;
    Lexer lexer(begin, end);

    /// Coalesce a list of comma separated literals.
    size_t num_literals_in_sequence = 0;
    bool prev_comma = false;

    while (true)
    {
        Token token = lexer.nextToken();

        if (!token.isSignificant())
            continue;

        /// Literals.
        if (token.type == TokenType::Number || token.type == TokenType::StringLiteral || token.type == TokenType::HereDoc)
        {
            if (0 == num_literals_in_sequence)
                hash.update("\x00", 1);
            ++num_literals_in_sequence;
            prev_comma = false;
            continue;
        }
        if (token.type == TokenType::Comma)
        {
            if (num_literals_in_sequence)
            {
                prev_comma = true;
                continue;
            }
        }
        else
        {
            if (num_literals_in_sequence > 1)
                hash.update("\x00", 1);

            if (prev_comma)
                hash.update(",", 1);

            num_literals_in_sequence = 0;
            prev_comma = false;
        }

        /// Slightly normalize something that look like aliases - if they are complex, replace them to `?` placeholders.
        if (token.type == TokenType::QuotedIdentifier
            /// Differentiate identifier from function (example: SHA224(x)).
            /// However, it does not account for whitespaces and comments between the function name and the parentheses.
            || (token.type == TokenType::BareWord && (token.end == end || *token.end != '(')))
        {
            /// Explicitly ask to keep identifier names
            if (keep_names)
            {
                hash.update(token.begin, token.size());
            }
            else
            {
                /// Identifier is complex if it contains whitespace or more than two digits
                /// or it's at least 36 bytes long (UUID for example).
                size_t num_digits = 0;

                const char * pos = token.begin;
                if (token.size() < 36)
                {
                    for (; pos != token.end; ++pos)
                    {
                        if (isWhitespaceASCII(*pos))
                            break;

                        if (isNumericASCII(*pos))
                        {
                            ++num_digits;
                            if (num_digits > 2)
                                break;
                        }
                    }
                }

                if (pos == token.end)
                    hash.update(token.begin, token.size());
                else
                    hash.update("\x01", 1);
            }

            continue;
        }

        if (token.isEnd() || token.isError())
            break;

        hash.update(token.begin, token.size());
    }

    return hash.get64();
}

UInt64 normalizedQueryHash(const String & query, bool keep_names)
{
    return normalizedQueryHash(query.data(), query.data() + query.size(), keep_names);
}

/// same token rules as normalizedQueryHash, but the token hashes are summed, so their order does not matter;
/// a sum rather than xor, so that a repeated token does not cancel out
UInt64 normalizedQueryHashUnordered(const char * begin, const char * end)
{
    UInt64 sum = 0;
    Lexer lexer(begin, end);

    /// Coalesce a list of comma separated literals.
    size_t num_literals_in_sequence = 0;
    bool prev_comma = false;

    while (true)
    {
        Token token = lexer.nextToken();

        if (!token.isSignificant())
            continue;

        /// Literals.
        if (token.type == TokenType::Number || token.type == TokenType::StringLiteral || token.type == TokenType::HereDoc)
        {
            if (0 == num_literals_in_sequence)
                sum += sipHash64("\x00", 1);
            ++num_literals_in_sequence;
            prev_comma = false;
            continue;
        }
        if (token.type == TokenType::Comma)
        {
            if (num_literals_in_sequence)
            {
                prev_comma = true;
                continue;
            }
        }
        else
        {
            if (num_literals_in_sequence > 1)
                sum += sipHash64("\x00", 1);

            if (prev_comma)
                sum += sipHash64(",", 1);

            num_literals_in_sequence = 0;
            prev_comma = false;
        }

        /// Slightly normalize something that look like aliases - if they are complex, replace them to `?` placeholders.
        if (token.type == TokenType::QuotedIdentifier
            /// Differentiate identifier from function (example: SHA224(x)).
            || (token.type == TokenType::BareWord && (token.end == end || *token.end != '(')))
        {
            /// Identifier is complex if it contains whitespace or more than two digits
            /// or it's at least 36 bytes long (UUID for example).
            size_t num_digits = 0;

            const char * pos = token.begin;
            if (token.size() < 36)
            {
                for (; pos != token.end; ++pos)
                {
                    if (isWhitespaceASCII(*pos))
                        break;

                    if (isNumericASCII(*pos))
                    {
                        ++num_digits;
                        if (num_digits > 2)
                            break;
                    }
                }
            }

            if (pos == token.end)
                sum += sipHash64(token.begin, token.size());
            else
                sum += sipHash64("\x01", 1);

            continue;
        }

        /// unlike normalizedQueryHash, an error token is hashed as well, and the lexer moves on past it
        if (token.isEnd())
            break;

        sum += sipHash64(token.begin, token.size());
    }

    return sum;
}


void normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data, bool keep_names)
{
    Lexer lexer(begin, end);
    /// Coalesce whitespace characters and comments to a single whitespace.
    bool prev_insignificant = false;

    /// Coalesce a list of comma separated literals to a single '?..' sequence.
    size_t num_literals_in_sequence = 0;
    bool prev_comma = false;
    bool prev_whitespace = false;

    while (true)
    {
        Token token = lexer.nextToken();

        if (!token.isSignificant())
        {
            /// Replace a sequence of insignificant tokens with single whitespace.
            if (!prev_insignificant)
            {
                if (0 == num_literals_in_sequence)
                {
                    // If it's leading whitespace, ignore it altogether.
                    if (token.begin != begin)
                    {
                        res_data.push_back(' ');
                    }
                }
                else
                {
                    prev_whitespace = true;
                }
            }
            prev_insignificant = true;
            continue;
        }

        prev_insignificant = false;

        /// Literals.
        if (token.type == TokenType::Number || token.type == TokenType::StringLiteral || token.type == TokenType::HereDoc)
        {
            if (0 == num_literals_in_sequence)
                res_data.push_back('?');
            ++num_literals_in_sequence;
            prev_whitespace = false;
            prev_comma = false;
            continue;
        }
        if (token.type == TokenType::Comma)
        {
            if (num_literals_in_sequence)
            {
                prev_comma = true;
                continue;
            }
        }
        else if (prev_comma && (token.type == TokenType::Plus || token.type == TokenType::Minus))
            continue;
        else
        {
            if (num_literals_in_sequence > 1)
            {
                res_data.push_back('.');
                res_data.push_back('.');
            }

            if (prev_comma)
                res_data.push_back(',');

            if (prev_whitespace)
                res_data.push_back(' ');

            num_literals_in_sequence = 0;
            prev_comma = false;
            prev_whitespace = false;
        }

        /// Slightly normalize something that look like aliases - if they are complex, replace them to `?` placeholders.
        if (token.type == TokenType::QuotedIdentifier
            /// Differentiate identifier from function (example: SHA224(x)).
            /// However, it does not account for whitespaces and comments between the function name and the parentheses.
            || (token.type == TokenType::BareWord && (token.end == end || *token.end != '(')))
        {
            /// Explicitly ask to normalize with identifier names
            if (keep_names)
            {
                res_data.insert(token.begin, token.end);
            }
            else
            {
                /// Identifier is complex if it contains whitespace or more than two digits
                /// or it's at least 36 bytes long (UUID for example).
                size_t num_digits = 0;

                const char * pos = token.begin;
                if (token.size() < 36)
                {
                    for (; pos != token.end; ++pos)
                    {
                        if (isWhitespaceASCII(*pos))
                            break;

                        if (isNumericASCII(*pos))
                        {
                            ++num_digits;
                            if (num_digits > 2)
                                break;
                        }
                    }
                }

                if (pos == token.end)
                {
                    res_data.insert(token.begin, token.end);
                }
                else
                {
                    res_data.push_back('`');
                    res_data.push_back('?');
                    res_data.push_back('`');
                }
            }

            continue;
        }

        if (token.isEnd() || token.isError())
            break;

        res_data.insert(token.begin, token.end);
    }
}

}
