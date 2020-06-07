#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{


bool ParserOptimizeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, Ranges * ranges)
{
    ParserKeyword s_optimize_table("OPTIMIZE TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_final("FINAL");
    ParserKeyword s_deduplicate("DEDUPLICATE");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ParserPartition partition_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr partition;
    bool final = false;
    bool deduplicate = false;
    String cluster_str;

    if (!s_optimize_table.ignore(pos, expected, ranges))
        return false;

    if (!name_p.parse(pos, table, expected, ranges))
        return false;

    if (s_dot.ignore(pos, expected, ranges))
    {
        database = table;
        if (!name_p.parse(pos, table, expected, ranges))
            return false;
    }

    if (ParserKeyword{"ON"}.ignore(pos, expected, ranges) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected, ranges))
        return false;

    if (s_partition.ignore(pos, expected, ranges))
    {
        if (!partition_p.parse(pos, partition, expected, ranges))
            return false;
    }

    if (s_final.ignore(pos, expected, ranges))
        final = true;

    if (s_deduplicate.ignore(pos, expected, ranges))
        deduplicate = true;

    auto query = std::make_shared<ASTOptimizeQuery>();
    node = query;

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    query->cluster = cluster_str;
    query->partition = partition;
    query->final = final;
    query->deduplicate = deduplicate;

    return true;
}


}
