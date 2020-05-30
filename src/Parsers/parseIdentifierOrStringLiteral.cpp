#include "parseIdentifierOrStringLiteral.h"

#include "ExpressionElementParsers.h"
#include "ASTLiteral.h"
#include "ASTIdentifier.h"
#include <Parsers/CommonParsers.h>
#include <Common/typeid_cast.h>

namespace DB
{

bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    ASTPtr res;

    if (!ParserIdentifier().parse(pos, res, expected))
    {
        if (!ParserStringLiteral().parse(pos, res, expected))
            return false;

        result = res->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
        result = getIdentifierName(res);

    return true;
}


bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        Strings strs;
        do
        {
            String str;
            if (!parseIdentifierOrStringLiteral(pos, expected, str))
                return false;

            strs.push_back(std::move(str));
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected));

        result = std::move(strs);
        return true;
    });
}

}
