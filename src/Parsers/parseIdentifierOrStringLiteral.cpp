#include "parseIdentifierOrStringLiteral.h"

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/typeid_cast.h>

namespace DB
{
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
            return true;
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

}
