#include <Parsers/ASTUndropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserUndropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
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

void registerStatementUndrop(StatementFactory & factory)
{
    factory.registerStatement("UNDROP",
    {
        .description = R"DOCS_MD(
Cancels the dropping of the table.

Beginning with ClickHouse version 23.3, you can recover a table in an `Atomic` database with `UNDROP TABLE`
during the period set by [`database_atomic_delay_before_drop_table_sec`](/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec) (8 minutes by default) after issuing `DROP TABLE`.
Tables dropped from `Atomic` databases are listed in `system.dropped_tables`.

For a `Shared` database in ClickHouse Cloud, this period is instead controlled by [`database_shared_drop_table_delay_seconds`](/reference/settings/session-settings/database#database_shared_drop_table_delay_seconds), which defaults to 8 hours.
Tables dropped from `Shared` databases aren't listed in `system.dropped_tables`.

If you have a materialized view without a `TO` clause associated with the dropped table, then you will also have to UNDROP the inner table of that view.

<Tip>
Also see [DROP TABLE](/reference/statements/drop)
</Tip>

Syntax:

```sql
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
```

**Example**

```sql
CREATE TABLE tab
(
    `id` UInt8
)
ENGINE = MergeTree
ORDER BY id;

DROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;
```

```response
Row 1:
──────
index:                 0
database:              default
table:                 tab
uuid:                  aa696a1a-1d70-4e60-a841-4c80827706cc
engine:                MergeTree
metadata_dropped_path: /var/lib/clickhouse/metadata_dropped/default.tab.aa696a1a-1d70-4e60-a841-4c80827706cc.sql
table_dropped_time:    2023-04-05 14:12:12

1 row in set. Elapsed: 0.001 sec.
```

```sql
UNDROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;

```response
Ok.

0 rows in set. Elapsed: 0.001 sec.
```

```sql
DESCRIBE TABLE tab
FORMAT Vertical;
```

```response
Row 1:
──────
name:               id
type:               UInt8
default_type:
default_expression:
comment:
codec_expression:
ttl_expression:
```
)DOCS_MD",
        .syntax = R"(
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
)",
        .related = {"DROP", "DETACH", "ATTACH"},
    });
}

}
