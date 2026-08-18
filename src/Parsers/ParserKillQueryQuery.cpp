#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String cluster_str;
    auto query = make_intrusive<ASTKillQueryQuery>();

    ParserKeyword p_kill{Keyword::KILL};
    ParserKeyword p_query{Keyword::QUERY};
    ParserKeyword p_mutation{Keyword::MUTATION};
    ParserKeyword p_part_move_to_shard{Keyword::PART_MOVE_TO_SHARD};
    ParserKeyword p_transaction{Keyword::TRANSACTION};
    ParserKeyword p_on{Keyword::ON};
    ParserKeyword p_test{Keyword::TEST};
    ParserKeyword p_sync{Keyword::SYNC};
    ParserKeyword p_async{Keyword::ASYNC};
    ParserKeyword p_where{Keyword::WHERE};
    ParserExpression p_where_expression;

    if (!p_kill.ignore(pos, expected))
        return false;

    if (p_query.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Query;
    else if (p_mutation.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Mutation;
    else if (p_part_move_to_shard.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::PartMoveToShard;
    else if (p_transaction.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Transaction;
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

namespace DB
{

void registerStatementKillQuery(StatementFactory & factory)
{
    factory.registerStatement("KILL",
    {
        .description = R"(
Terminates the queries or the mutations which match the filter expression. The queries to terminate are selected from
`system.processes`, the mutations from `system.mutations`.

`SYNC` waits until the queries are actually terminated, `ASYNC` returns immediately, and `TEST` only checks the access
rights and shows the list of the queries which would be terminated.
)",
        .syntax = R"(
KILL QUERY [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.processes query> [SYNC|ASYNC|TEST] [FORMAT format]
KILL MUTATION [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.mutations query> [TEST] [FORMAT format]
)",
        .examples = {
            {"Terminate a query by its identifier", "KILL QUERY WHERE query_id = '2-857d-4a57-9ee0-327da5d60a90';", ""},
            {"Terminate the mutations of a table", "KILL MUTATION WHERE database = 'default' AND table = 'table';", ""},
        },
        .related = {"SYSTEM", "SHOW", "ALTER"},
    });
}

}
