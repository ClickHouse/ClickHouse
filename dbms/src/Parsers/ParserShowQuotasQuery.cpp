#include <Parsers/ParserShowQuotasQuery.h>
#include <Parsers/ASTShowQuotasQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
bool ParserShowQuotasQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool usage;
    bool current;
    if (ParserKeyword{"SHOW QUOTAS"}.ignore(pos, expected))
    {
        usage = false;
        current = false;
    }
    else if (ParserKeyword{"SHOW QUOTA USAGE"}.ignore(pos, expected))
    {
        usage = true;
        if (ParserKeyword{"ALL"}.ignore(pos, expected))
        {
            current = false;
        }
        else
        {
            ParserKeyword{"CURRENT"}.ignore(pos, expected);
            current = true;
        }
    }
    else
        return false;

    auto query = std::make_shared<ASTShowQuotasQuery>();
    query->usage = usage;
    query->current = current;
    node = query;
    return true;
}
}
