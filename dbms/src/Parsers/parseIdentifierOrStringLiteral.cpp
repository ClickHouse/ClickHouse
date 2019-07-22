#include "parseIdentifierOrStringLiteral.h"

#include "ExpressionElementParsers.h"
#include "ASTLiteral.h"
#include "ASTIdentifier.h"
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
        result = *getIdentifierName(res);

    return true;
}

}
