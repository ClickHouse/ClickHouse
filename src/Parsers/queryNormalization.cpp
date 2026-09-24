#include <Parsers/Lexer.h>
#include <Parsers/queryNormalization.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace
{

/// looks generated: has whitespace, more than two digits, or is 36+ bytes long
bool isComplexIdentifier(const char * begin, const char * end)
{
    if (end - begin >= 36)
        return true;

    size_t num_digits = 0;
    for (const char * pos = begin; pos != end; ++pos)
    {
        if (isWhitespaceASCII(*pos))
            return true;

        if (isNumericASCII(*pos))
        {
            ++num_digits;
            if (num_digits > 2)
                return true;
        }
    }

    return false;
}

/// passes to emit the units normalizedQueryHash hashes, in the order of the query
template <typename Emit>
void forEachNormalizedUnit(const char * begin, const char * end, bool keep_names, bool stop_at_error, Emit && emit)
{
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
                emit("\x00", 1);
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
                emit("\x00", 1);

            if (prev_comma)
                emit(",", 1);

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
            if (keep_names || !isComplexIdentifier(token.begin, token.end))
                emit(token.begin, token.size());
            else
                emit("\x01", 1);

            continue;
        }

        if (token.isEnd() || (stop_at_error && token.isError()))
            break;

        emit(token.begin, token.size());
    }
}

}


UInt64 normalizedQueryHash(const char * begin, const char * end, bool keep_names)
{
    SipHash hash;
    forEachNormalizedUnit(begin, end, keep_names, /*stop_at_error=*/ true, [&](const char * data, size_t size) { hash.update(data, size); });
    return hash.get64();
}

UInt64 normalizedQueryHashUnordered(const char * begin, const char * end)
{
    /// a sum does not depend on the order, and unlike xor a repeated token does not cancel out
    UInt64 sum = 0;
    forEachNormalizedUnit(begin, end, /*keep_names=*/ false, /*stop_at_error=*/ false,
        [&](const char * data, size_t size) { sum += sipHash64(data, size); });
    return sum;
}

UInt64 normalizedQueryHash(const String & query, bool keep_names)
{
    return normalizedQueryHash(query.data(), query.data() + query.size(), keep_names);
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
            if (keep_names || !isComplexIdentifier(token.begin, token.end))
            {
                res_data.insert(token.begin, token.end);
            }
            else
            {
                res_data.push_back('`');
                res_data.push_back('?');
                res_data.push_back('`');
            }

            continue;
        }

        if (token.isEnd() || token.isError())
            break;

        res_data.insert(token.begin, token.end);
    }
}

}
