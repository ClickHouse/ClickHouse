#include <Parsers/Access/ParserShowAccessQuery.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool ParserShowAccessQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword("SHOW ACCESS").ignore(pos, expected))
        return false;

    bool show_rbac_version = false;
    if (ParserKeyword("WITH VERSION").ignore(pos, expected))
        show_rbac_version = true;

    auto query = std::make_shared<ASTShowAccessQuery>();
    node = query;

    query->show_rbac_version = show_rbac_version;
    return true;
}

}
