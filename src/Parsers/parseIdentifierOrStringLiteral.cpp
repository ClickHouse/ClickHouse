#include <Parsers/parseIdentifierOrStringLiteral.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        ASTPtr ast;
        if (ParserIdentifier().parse(pos, ast, expected))
        {
            result = getIdentifierName(ast);
            return true;
        }

        if (ParserStringLiteral().parse(pos, ast, expected))
        {
            result = ast->as<ASTLiteral &>().value.safeGet<String>();
            return !result.empty();
        }

        return false;
    });
}


bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result)
{
    Strings res;

    auto parse_single_id_or_literal = [&]
    {
        String str;
        if (!parseIdentifierOrStringLiteral(pos, expected, str))
            return false;

        res.emplace_back(std::move(str));
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_single_id_or_literal, false))
        return false;

    result = std::move(res);
    return true;
}


NameSet parseColumnNameList(std::string_view str)
{
    Tokens tokens(str.data(), str.data() + str.size());
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Expected expected;
    NameSet result;

    auto parse_column_name = [&]
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (ParserStringLiteral().parse(pos, ast, expected))
            {
                auto name = ast->as<ASTLiteral &>().value.safeGet<String>();
                /// An empty identifier does not even tokenize as one, but an empty string literal does: reject
                /// it here, the position of the furthest token would point past it.
                if (name.empty())
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_TEXT,
                        "Cannot parse {} as a comma-separated list of column names: a column name cannot be empty",
                        quoteString(str));
                result.insert(std::move(name));
                return true;
            }

            if (!ParserIdentifier().parse(pos, ast, expected))
                return false;

            String name = getIdentifierName(ast);
            while (ParserToken(TokenType::Dot).ignore(pos, expected))
            {
                if (!ParserIdentifier().parse(pos, ast, expected))
                    return false;
                name += '.';
                name += getIdentifierName(ast);
            }

            result.insert(std::move(name));
            return true;
        });
    };

    if (pos->type != TokenType::EndOfStream)
        ParserList::parseUtil(pos, expected, parse_column_name, /*allow_empty_=*/false);

    /// `parseUtil` stops before the first token that does not continue the list, e.g. at `b` in `a b` or
    /// at the trailing comma in `a,`. Report the furthest token the parser looked at, as `parseQuery` does:
    /// for `n.1` that is `1`, not the `n` the failed name started at.
    if (pos->type != TokenType::EndOfStream)
    {
        const Token & unexpected = pos.max();
        throw Exception(
            ErrorCodes::CANNOT_PARSE_TEXT,
            "Cannot parse {} as a comma-separated list of column names: unexpected {} at position {}. "
            "Separate the names with commas and write a name with special characters in backquotes",
            quoteString(str),
            unexpected.isEnd() ? "end of string" : quoteString(std::string_view(unexpected.begin, unexpected.end)),
            unexpected.begin - str.data() + 1);
    }

    return result;
}

}
