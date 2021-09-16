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

    ParserKeyword p_kill{"KILL"};
    ParserKeyword p_query{"QUERY"};
    ParserKeyword p_mutation{"MUTATION"};
    ParserKeyword p_part_move_to_shard{"PART_MOVE_TO_SHARD"};
    ParserKeyword p_on{"ON"};
    ParserKeyword p_test{"TEST"};
    ParserKeyword p_sync{"SYNC"};
    ParserKeyword p_async{"ASYNC"};
    ParserKeyword p_where{"WHERE"};
    ParserExpression p_where_expression;

    if (!p_kill.ignore(pos, expected))
        return false;

    if (p_query.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Query;
    else if (p_mutation.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Mutation;
    else if (p_part_move_to_shard.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::PartMoveToShard;
    else
        return false;

    if (p_on.ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;

    if (!p_where.ignore(pos, expected) || !p_where_expression.parse(pos, query->where_expression, expected))
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
