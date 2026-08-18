#include <Parsers/ASTUndropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserUndropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/StatementFactory.h>
#include <Core/UUID.h>


namespace DB
{

namespace
{

bool parseUndropQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_table(Keyword::TABLE);
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p(true);

    ASTPtr database;
    ASTPtr table;
    String cluster_str;
    /// We can specify the table's uuid for exact undrop.
    /// because the same name of a table can be created and deleted multiple times,
    /// and can generate multiple different uuids.
    UUID uuid = UUIDHelpers::Nil;

    if (!s_table.ignore(pos, expected))
        return false;
    if (!name_p.parse(pos, table, expected))
        return false;
    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }
    if (ParserKeyword(Keyword::UUID).ignore(pos, expected))
    {
        ParserStringLiteral uuid_p;
        ASTPtr ast_uuid;
        if (!uuid_p.parse(pos, ast_uuid, expected))
            return false;
        uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.safeGet<String>());
    }
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }
    auto query = make_intrusive<ASTUndropQuery>();
    node = query;

    query->database = database;
    query->table = table;
    query->uuid = uuid;

    if (database)
        query->children.push_back(database);

    chassert(table);
    query->children.push_back(table);

    query->cluster = cluster_str;

    return true;
}

}

bool ParserUndropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_undrop(Keyword::UNDROP);

    if (s_undrop.ignore(pos, expected))
        return parseUndropQuery(pos, node, expected);
    return false;
}

}

namespace DB
{

REGISTER_STATEMENTS(Undrop)
{
    factory.registerStatement("UNDROP",
    {
        .description = R"(
Cancels the dropping of a table. A table of an `Atomic` database can be recovered during the period set by the server
setting `database_atomic_delay_before_drop_table_sec` after `DROP TABLE` was issued. The tables which can still be
recovered are listed in `system.dropped_tables`.
)",
        .syntax = R"(
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
)",
        .examples = {{"Recover a dropped table", "UNDROP TABLE tab;", ""}},
        .related = {"DROP", "DETACH", "ATTACH"},
    });
}

}
