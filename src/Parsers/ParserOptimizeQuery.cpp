#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{

bool ParserOptimizeQueryColumnsSpecification::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    // Do not allow APPLY and REPLACE transformers.
    // Since we use Columns Transformers only to get list of columns,
    // we can't actually modify content of the columns for deduplication.
    const auto allowed_transformers = ParserColumnsTransformers::ColumnTransformers{ParserColumnsTransformers::ColumnTransformer::EXCEPT};

    return ParserColumnsMatcher(allowed_transformers).parse(pos, node, expected)
        || ParserAsterisk(allowed_transformers).parse(pos, node, expected)
        || ParserIdentifier(false).parse(pos, node, expected);
}


bool ParserOptimizeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_optimize_table("OPTIMIZE TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_final("FINAL");
    ParserKeyword s_deduplicate("DEDUPLICATE");
    ParserKeyword s_by("BY");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ParserPartition partition_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr partition;
    bool final = false;
    bool deduplicate = false;
    String cluster_str;

    if (!s_optimize_table.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    if (ParserKeyword{"ON"}.ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_p.parse(pos, partition, expected))
            return false;
    }

    if (s_final.ignore(pos, expected))
        final = true;

    if (s_deduplicate.ignore(pos, expected))
        deduplicate = true;

    ASTPtr deduplicate_by_columns;
    if (deduplicate && s_by.ignore(pos, expected))
    {
        if (!ParserList(std::make_unique<ParserOptimizeQueryColumnsSpecification>(), std::make_unique<ParserToken>(TokenType::Comma), false)
                .parse(pos, deduplicate_by_columns, expected))
            return false;
    }

    auto query = std::make_shared<ASTOptimizeQuery>();
    node = query;

    tryGetIdentifierNameInto(database, query->database);
    tryGetIdentifierNameInto(table, query->table);

    query->cluster = cluster_str;
    if ((query->partition = partition))
        query->children.push_back(partition);
    query->final = final;
    query->deduplicate = deduplicate;
    query->deduplicate_by_columns = deduplicate_by_columns;

    return true;
}


}
