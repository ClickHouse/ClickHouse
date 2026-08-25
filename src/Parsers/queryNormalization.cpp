#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/queryNormalization.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <algorithm>
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

/// Whether the elements of `list`, which is a child of `parent`, can be reordered
/// without changing what the query does.
bool isUnorderedList(const IAST & parent, const IAST & list)
{
    if (const auto * select = parent.as<ASTSelectQuery>())
    {
        if (select->select().get() == &list)
            return true;

        /// `ROLLUP` gives a meaning to the order of the keys, `CUBE` and `GROUPING SETS` do not.
        if (select->groupBy().get() == &list)
            return !select->group_by_with_rollup;

        return false;
    }

    if (const auto * function = parent.as<ASTFunction>())
        return function->arguments.get() == &list && (function->name == "and" || function->name == "or");

    return false;
}

/// A non-empty list where every element is a literal, such as the right hand side of `IN`.
bool isListOfLiterals(const IAST & ast)
{
    if (!ast.as<ASTExpressionList>() || ast.children.empty())
        return false;

    for (const auto & child : ast.children)
        if (!child->as<ASTLiteral>())
            return false;

    return true;
}

IASTHash hashCanonical(const IAST & ast, bool sort_children)
{
    checkStackSize();

    SipHash hash;

    /// Values of literals are erased, like in `normalizedQueryHash`.
    if (ast.as<ASTLiteral>())
    {
        hash.update("\x00", 1);
        return getSipHash128AsPair(hash);
    }

    /// A list of literals is collapsed, so that `IN (1, 2)` and `IN (1, 2, 3)` match.
    if (isListOfLiterals(ast))
    {
        hash.update("\x00", 1);
        if (ast.children.size() > 1)
            hash.update("\x00", 1);
        return getSipHash128AsPair(hash);
    }

    if (const auto * identifier = ast.as<ASTIdentifier>();
        identifier && isComplexIdentifier(identifier->full_name.data(), identifier->full_name.data() + identifier->full_name.size()))
    {
        hash.update("\x01", 1);
        return getSipHash128AsPair(hash);
    }

    ast.updateTreeHashImpl(hash, /*ignore_aliases=*/ true);

    std::vector<IASTHash> child_hashes;
    child_hashes.reserve(ast.children.size());
    for (const auto & child : ast.children)
        child_hashes.push_back(hashCanonical(*child, isUnorderedList(ast, *child)));

    if (sort_children)
        std::sort(child_hashes.begin(), child_hashes.end());

    for (const auto & child_hash : child_hashes)
        hash.update(child_hash);

    return getSipHash128AsPair(hash);
}

}

UInt64 canonicalQueryHash(const IAST & ast)
{
    return CityHash_v1_0_2::Hash128to64(hashCanonical(ast, /*sort_children=*/ false));
}

}
