#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Core/Field.h>
#include <Parsers/ASTLiteral.h>
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

/// normalizedQueryHash erases Number and StringLiteral tokens only, so NULL and true stay distinct
bool isErasedByLexer(Field::Types::Which which)
{
    switch (which)
    {
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64:
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UInt256:
        case Field::Types::Int256:
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
        case Field::Types::String:
            return true;
        default:
            return false;
    }
}

/// a generated-looking name becomes a placeholder, like normalizedQueryHash does, one SQL token at a time
void updateWithName(SipHash & hash, const String & name)
{
    if (isComplexIdentifier(name.data(), name.data() + name.size()))
    {
        hash.update("\x01", 1);
        return;
    }

    hash.update(name.size());
    hash.update(name);
}

/// such as the right hand side of IN
bool isListOfErasedLiterals(const IAST & ast)
{
    if (!ast.as<ASTExpressionList>() || ast.children.empty())
        return false;

    for (const auto & child : ast.children)
    {
        const auto * literal = child->as<ASTLiteral>();
        if (!literal || !isErasedByLexer(literal->value.getType()) || !literal->tryGetAlias().empty())
            return false;
    }

    return true;
}

IASTHash hashUnordered(const IAST & ast)
{
    checkStackSize();

    SipHash hash;
    updateWithName(hash, ast.tryGetAlias());

    if (const auto * literal = ast.as<ASTLiteral>())
    {
        const auto which = literal->value.getType();

        /// erase the value, same as normalizedQueryHash
        if (isErasedByLexer(which))
        {
            hash.update("\x00", 1);
            return getSipHash128AsPair(hash);
        }

        /// the lexer turns a collection of literals into ?.. as well, so keep only its kind
        if (!Field::isScalar(which))
        {
            hash.update("\x00", 1);
            hash.update(which);
            return getSipHash128AsPair(hash);
        }
    }

    /// collapse it, so that IN (1, 2) and IN (1, 2, 3) match
    if (isListOfErasedLiterals(ast))
    {
        hash.update("\x00", 1);
        if (ast.children.size() > 1)
            hash.update("\x00", 1);
        return getSipHash128AsPair(hash);
    }

    /// as<> only matches the exact type, and the lexer sees every part as its own token, so db1.t34 is two simple names
    if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(&ast))
    {
        hash.update("Identifier");
        hash.update(identifier->name_parts.size());
        for (const auto & part : identifier->name_parts)
            updateWithName(hash, part);
    }
    else
        ast.updateTreeHashImpl(hash, /*ignore_aliases=*/ true);

    std::vector<IASTHash> child_hashes;
    child_hashes.reserve(ast.children.size());
    for (const auto & child : ast.children)
        child_hashes.push_back(hashUnordered(*child));

    if (ast.as<ASTExpressionList>())
        std::sort(child_hashes.begin(), child_hashes.end());

    for (const auto & child_hash : child_hashes)
        hash.update(child_hash);

    return getSipHash128AsPair(hash);
}

}

UInt64 unorderedQueryHash(const IAST & ast)
{
    return CityHash_v1_0_2::Hash128to64(hashUnordered(ast));
}

}
