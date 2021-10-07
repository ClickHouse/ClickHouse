#pragma once

#include <common/types.h>

#include <Core/Defines.h>
#include <Parsers/Lexer.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
template <bool keep_names>
inline UInt64 ALWAYS_INLINE normalizedQueryHash(const char * begin, const char * end)
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
        if (token.type == TokenType::Number || token.type == TokenType::StringLiteral)
        {
            if (0 == num_literals_in_sequence)
                hash.update("\x00", 1);
            ++num_literals_in_sequence;
            prev_comma = false;
            continue;
        }
        else if (token.type == TokenType::Comma)
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
            /// By the way, there is padding in columns and pointer dereference is Ok.
            || (token.type == TokenType::BareWord && *token.end != '('))
        {
            /// Explicitly ask to keep identifier names
            if constexpr (keep_names)
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

template <bool keep_names>
inline UInt64 ALWAYS_INLINE normalizedQueryHash(const String & query)
{
    return normalizedQueryHash<keep_names>(query.data(), query.data() + query.size());
}


template <bool keep_names>
inline void ALWAYS_INLINE normalizeQueryToPODArray(const char * begin, const char * end, PaddedPODArray<UInt8> & res_data)
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
        if (token.type == TokenType::Number || token.type == TokenType::StringLiteral)
        {
            if (0 == num_literals_in_sequence)
                res_data.push_back('?');
            ++num_literals_in_sequence;
            prev_whitespace = false;
            prev_comma = false;
            continue;
        }
        else if (token.type == TokenType::Comma)
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
            /// By the way, there is padding in columns and pointer dereference is Ok.
            || (token.type == TokenType::BareWord && *token.end != '('))
        {
            /// Explicitly ask to normalize with identifier names
            if constexpr (keep_names)
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
    res_data.push_back(0);
}

}
