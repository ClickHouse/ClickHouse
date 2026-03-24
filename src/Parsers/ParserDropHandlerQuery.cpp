#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropHandlerQuery.h>
#include <Parsers/ASTDropHandlerQuery.h>


namespace DB
{

bool ParserDropHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr handler_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!name_p.parse(pos, handler_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTDropHandlerQuery>();
    tryGetIdentifierNameInto(handler_name, query->handler_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);

    node = query;
    return true;
}

}
