#include <Parsers/ParserDropHandlerQuery.h>

#include <Parsers/ASTDropHandlerQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

bool ParserDropHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDropHandlerQuery>();

    if (s_if_exists.ignore(pos, expected))
        query->if_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;
    query->handler_name = getIdentifierName(name_ast);

    if (s_on.ignore(pos, expected))
    {
        String cluster_str;
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        query->cluster = std::move(cluster_str);
    }

    node = query;
    return true;
}

}
