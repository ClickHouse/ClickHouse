#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    String cluster_str;
    auto query = std::make_shared<ASTKillQueryQuery>();

    ParserKeyword p_kill{"KILL"};
    ParserKeyword p_query{"QUERY"};
    ParserKeyword p_mutation{"MUTATION"};
    ParserKeyword p_on{"ON"};
    ParserKeyword p_test{"TEST"};
    ParserKeyword p_sync{"SYNC"};
    ParserKeyword p_async{"ASYNC"};
    ParserKeyword p_where{"WHERE"};
    ParserExpression p_where_expression;

    if (!p_kill.ignore(pos, expected, ranges))
        return false;

    if (p_query.ignore(pos, expected, ranges))
        query->type = ASTKillQueryQuery::Type::Query;
    else if (p_mutation.ignore(pos, expected, ranges))
        query->type = ASTKillQueryQuery::Type::Mutation;
    else
        return false;

    if (p_on.ignore(pos, expected, ranges) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected, ranges))
        return false;

    if (!p_where.ignore(pos, expected, ranges) || !p_where_expression.parse(pos, query->where_expression, expected, ranges))
        return false;

    if (p_sync.ignore(pos, expected, ranges))
        query->sync = true;
    else if (p_async.ignore(pos, expected, ranges))
        query->sync = false;
    else if (p_test.ignore(pos, expected, ranges))
        query->test = true;

    query->cluster = cluster_str;
    query->children.emplace_back(query->where_expression);
    node = std::move(query);
    return true;
}

}
