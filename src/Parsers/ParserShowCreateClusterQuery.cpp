#include <Parsers/ASTShowCreateClusterQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserShowCreateClusterQuery.h>


namespace DB
{

bool ParserShowCreateClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_cluster(Keyword::CLUSTER);
    ParserIdentifier name_p;

    if (!s_show.ignore(pos, expected))
        return false;
    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_cluster.ignore(pos, expected))
        return false;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;

    auto query = make_intrusive<ASTShowCreateClusterQuery>();
    tryGetIdentifierNameInto(name_ast, query->cluster_name);
    node = query;
    return true;
}

}
