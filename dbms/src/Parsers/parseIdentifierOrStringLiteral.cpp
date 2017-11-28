#include "parseIdentifierOrStringLiteral.h"

#include "ExpressionElementParsers.h"
#include "ASTLiteral.h"
#include "ASTIdentifier.h"
#include <Common/typeid_cast.h>

namespace DB
{

bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result)
{
    IParser::Pos begin = pos;
    ASTPtr res;

    if (!ParserIdentifier().parse(pos, res, expected))
    {
        pos = begin;
        if (!ParserStringLiteral().parse(pos, res, expected))
            return false;

        result = typeid_cast<const ASTLiteral &>(*res).value.safeGet<String>();
    }
    else
        result = typeid_cast<const ASTIdentifier &>(*res).name;

    return true;
}

}