#include <Parsers/ASTExpressionList.h>
#include <Parsers/Lexer.h>
#include <Parsers/queryNormalization.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <algorithm>
#include <utility>
#include <vector>


namespace DB
{

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
            if (keep_names || !isComplexIdentifier(token.begin, token.end))
                hash.update(token.begin, token.size());
            else
                hash.update("\x01", 1);

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

namespace
{

/// normalized first, so that elements erased to the same placeholder end up adjacent, then the plain text,
/// because a tie on the normalized form alone would leave two elements the final hash separates in input order
std::pair<String, String> sortKey(const IAST & ast)
{
    String text = ast.formatWithSecretsOneLine();

    PaddedPODArray<UInt8> normalized;
    normalizeQueryToPODArray(text.data(), text.data() + text.size(), normalized, /*keep_names=*/ false);
    return {String(normalized.begin(), normalized.end()), std::move(text)};
}

void sortExpressionLists(IAST & ast)
{
    checkStackSize();

    for (const auto & child : ast.children)
        sortExpressionLists(*child);

    if (!ast.as<ASTExpressionList>())
        return;

    std::vector<std::pair<std::pair<String, String>, ASTPtr>> sorted;
    sorted.reserve(ast.children.size());
    for (const auto & element : ast.children)
        sorted.emplace_back(sortKey(*element), element);

    std::sort(sorted.begin(), sorted.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (size_t i = 0; i < sorted.size(); ++i)
        ast.children[i] = sorted[i].second;
}

}

UInt64 unorderedQueryHash(const IAST & ast)
{
    ASTPtr sorted = ast.clone();
    sortExpressionLists(*sorted);

    /// the plain text, not the normalized one - normalizing twice would read the placeholders back as SQL
    return normalizedQueryHash(sorted->formatWithSecretsOneLine(), /*keep_names=*/ false);
}

}
