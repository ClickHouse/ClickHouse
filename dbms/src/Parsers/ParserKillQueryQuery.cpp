#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String cluster_str;
    auto query = std::make_shared<ASTKillQueryQuery>();

    ParserKeyword p_on{"ON"};
    ParserKeyword p_test{"TEST"};
    ParserKeyword p_sync{"SYNC"};
    ParserKeyword p_async{"ASYNC"};
    ParserKeyword p_where{"WHERE"};
    ParserKeyword p_kill_query{"KILL QUERY"};
    ParserExpression p_where_expression;

    if (!p_kill_query.ignore(pos, expected))
        return false;

    if (p_on.ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;

    if (p_where.ignore(pos, expected) && !p_where_expression.parse(pos, query->where_expression, expected))
        return false;

    if (p_sync.ignore(pos, expected))
        query->sync = true;
    else if (p_async.ignore(pos, expected))
        query->sync = false;
    else if (p_test.ignore(pos, expected))
        query->test = true;

    query->cluster = cluster_str;
    query->children.emplace_back(query->where_expression);
    node = std::move(query);
    return true;
}

}
