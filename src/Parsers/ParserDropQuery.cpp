#include <Parsers/ASTDropQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

bool parseDropQuery(IParser::Pos & pos, ASTPtr & node, Expected & expected, const ASTDropQuery::Kind kind)
{
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_dictionary(Keyword::DICTIONARY);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_tables(Keyword::TABLES);
    ParserKeyword s_not(Keyword::NOT);
    ParserKeyword s_like(Keyword::LIKE);
    ParserKeyword s_ilike(Keyword::ILIKE);
    ParserToken s_dot(TokenType::Dot);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_if_empty(Keyword::IF_EMPTY);
    ParserIdentifier name_p(true);
    ParserStringLiteral like_p(Highlight::string_like);
    ParserKeyword s_permanently(Keyword::PERMANENTLY);
    ParserKeyword s_no_delay(Keyword::NO_DELAY);
    ParserKeyword s_sync(Keyword::SYNC);
    ParserNameList tables_p;

    ASTPtr database;
    ASTPtr database_and_tables;
    String cluster_str;
    ASTPtr like;
    bool if_exists = false;
    bool if_empty = false;
    bool has_tables = false;
    bool is_like = false;
    bool is_not_like = false;
    bool is_case_insensitive_like = false;
    bool has_all = false;
    bool temporary = false;
    bool is_dictionary = false;
    bool is_view = false;
    bool sync = false;
    bool permanently = false;

    if (s_all.checkWithoutMoving(pos, expected))
        has_all = true;

    if (s_database.ignore(pos, expected))
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (s_if_empty.ignore(pos, expected))
            if_empty = true;

        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else if ((s_tables.ignore(pos, expected) || (s_all.ignore(pos, expected) && s_tables.ignore(pos, expected))) && kind == ASTDropQuery::Kind::Truncate)
    {
        /// Either 'TRUNCATE TABLES FROM ..' or 'TRUNCATE ALL TABLES FROM ..'
        has_tables = true;
        if (!s_from.ignore(pos, expected))
            return false;

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (!name_p.parse(pos, database, expected))
            return false;

        bool not_like = false;
        if (s_not.ignore(pos, expected))
            not_like = true;

        if (s_like.ignore(pos, expected))
        {
            if (not_like)
                is_not_like = true;
            if (!like_p.parse(pos, like, expected))
                return false;
            is_like = true;
        }

        if (s_ilike.ignore(pos, expected))
        {
            is_case_insensitive_like = true;
            if (not_like)
                is_not_like = true;
            if (!like_p.parse(pos, like, expected))
                return false;
            is_like = true;
        }
    }
    else
    {
        if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (s_view.ignore(pos, expected))
            is_view = true;
        else if (s_dictionary.ignore(pos, expected))
            is_dictionary = true;

        /// for TRUNCATE queries TABLE keyword is assumed as default and can be skipped
        if (!is_view && !is_dictionary && (!s_table.ignore(pos, expected) && kind != ASTDropQuery::Kind::Truncate))
        {
            return false;
        }

        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        if (s_if_empty.ignore(pos, expected))
            if_empty = true;

        if (!tables_p.parse(pos, database_and_tables, expected))
            return false;

        if (database_and_tables->as<ASTExpressionList &>().children.size() > 1 && kind != ASTDropQuery::Kind::Drop)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Only Support DROP multiple tables currently");
    }

    /// common for tables / dictionaries / databases
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (kind == ASTDropQuery::Kind::Detach && s_permanently.ignore(pos, expected))
        permanently = true;

    /// actually for TRUNCATE NO DELAY / SYNC means nothing
    if (s_no_delay.ignore(pos, expected) || s_sync.ignore(pos, expected))
        sync = true;

    auto query = make_intrusive<ASTDropQuery>();
    node = query;

    query->kind = kind;
    query->if_exists = if_exists;
    query->if_empty = if_empty;
    query->has_tables = has_tables;
    query->has_all = has_all;
    query->setIsTemporary(temporary);
    query->is_dictionary = is_dictionary;
    query->is_view = is_view;
    query->sync = sync;
    query->permanently = permanently;
    query->database = database;
    query->database_and_tables = database_and_tables;
    query->case_insensitive_like = is_case_insensitive_like;
    query->not_like = is_not_like;

    if (database)
        query->children.push_back(database);

    if (database_and_tables)
        query->children.push_back(database_and_tables);

    if (is_like)
    {
        query->like = like->as<ASTLiteral &>().value.safeGet<String>();
        query->has_like = true;
    }

    query->cluster = cluster_str;

    if (database_and_tables && database_and_tables->as<ASTExpressionList &>().children.size() == 1)
        node = query->getRewrittenASTsOfSingleTable(query)[0];

    return true;
}

}

bool ParserDropQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_detach(Keyword::DETACH);
    ParserKeyword s_truncate(Keyword::TRUNCATE);

    if (s_drop.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Drop);
    if (s_detach.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Detach);
    if (s_truncate.ignore(pos, expected))
        return parseDropQuery(pos, node, expected, ASTDropQuery::Kind::Truncate);
    return false;
}

}

namespace DB
{

void registerStatementDrop(StatementFactory & factory)
{
    factory.registerStatement("DROP",
    {
        .description = R"DOCS_MD(
Deletes existing entity. If the `IF EXISTS` clause is specified, these queries do not return an error if the entity does not exist. If the `SYNC` modifier is specified, the entity is dropped without delay.

## DROP DATABASE {#drop-database}

Deletes all tables inside the `db` database, then deletes the `db` database itself.

Syntax:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

## DROP TABLE {#drop-table}

Deletes one or more tables.

<Tip>
To undo the deletion of a table, please see [UNDROP TABLE](/reference/statements/undrop)
</Tip>

Syntax:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Limitations:
- If the clause `IF EMPTY` is specified, the server checks the emptiness of the table only on the replica which received the query.
- Deleting multiple tables at once is not an atomic operation, i.e. if the deletion of a table fails, subsequent tables will not be deleted.

## DROP DICTIONARY {#drop-dictionary}

Deletes the dictionary.

Syntax:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

## DROP USER {#drop-user}

Deletes a user.

Syntax:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROLE {#drop-role}

Deletes a role. The deleted role is revoked from all the entities where it was assigned.

Syntax:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROW POLICY {#drop-row-policy}

Deletes a row policy. Deleted row policy is revoked from all the entities where it was assigned.

Syntax:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP MASKING POLICY {#drop-masking-policy}

Deletes a masking policy.

Syntax:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP QUOTA {#drop-quota}

Deletes a quota. The deleted quota is revoked from all the entities where it was assigned.

Syntax:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP SETTINGS PROFILE {#drop-settings-profile}

Deletes a settings profile. The deleted settings profile is revoked from all the entities where it was assigned.

Syntax:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP VIEW {#drop-view}

Deletes a view. Views can be deleted by a `DROP TABLE` command as well but `DROP VIEW` checks that `[db.]name` is a view.

Syntax:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

## DROP FUNCTION {#drop-function}

Deletes a user defined function created by [CREATE FUNCTION](/reference/statements/create/function).
System functions can not be dropped.

**Syntax**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Example**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

## DROP NAMED COLLECTION {#drop-named-collection}

Deletes a named collection.

**Syntax**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```
)DOCS_MD",
        .syntax = R"(
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY] [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
DROP FUNCTION [IF EXISTS] function_name [ON CLUSTER cluster]
DROP NAMED COLLECTION [IF EXISTS] name [ON CLUSTER cluster]
)",
        .related = {"DETACH", "TRUNCATE", "UNDROP", "CREATE"},
    });

    factory.registerStatement("DETACH",
    {
        .description = R"DOCS_MD(
Makes the server "forget" about the existence of a table, a materialized view, a dictionary, or a database.

**Syntax**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

Detaching does not delete the data or metadata of a table, a materialized view, a dictionary or a database. If an entity was not detached `PERMANENTLY`, on the next server launch the server will read the metadata and recall the table/view/dictionary/database again. If an entity was detached `PERMANENTLY`, there will be no automatic recall.

Whether a table, a dictionary or a database was detached permanently or not, in both cases you can reattach them using the [ATTACH](/reference/statements/attach) query.
System log tables can be also attached back (e.g. `query_log`, `text_log`, etc.). Other system tables can't be reattached. On the next server launch the server will recall those tables again.

`ATTACH MATERIALIZED VIEW` does not work with short syntax (without `SELECT`), but you can attach it using the `ATTACH TABLE` query.

Note that you can not detach permanently the table which is already detached (temporary). But you can attach it back and then detach permanently again.

Also, you can not [DROP](/reference/statements/drop#drop-table) the detached table, or [CREATE TABLE](/reference/statements/create/table) with the same name as detached permanently, or replace it with the other table with [RENAME TABLE](/reference/statements/rename) query.

The `SYNC` modifier executes the action without delay.

**Example**

Creating a table:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Detaching the table:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

<Note>
In ClickHouse Cloud users should use the `PERMANENTLY` clause e.g. `DETACH TABLE <table> PERMANENTLY`. If this clause is not used, tables will be reattached on cluster restart e.g. during upgrades.
</Note>

**See Also**

- [Materialized View](/reference/statements/create/view#materialized-view)
- [Dictionaries](/reference/statements/create/dictionary)
)DOCS_MD",
        .syntax = R"(
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
)",
        .related = {"ATTACH", "DROP"},
    });

    factory.registerStatement("TRUNCATE",
    {
        .description = R"DOCS_MD(
The `TRUNCATE` statement in ClickHouse is used to quickly remove all data from a table or database while preserving their structure.

## TRUNCATE TABLE {#truncate-table}
```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```
<br/>
| Parameter           | Description                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------|
| `IF EXISTS`         | Prevents an error if the table does not exist. If omitted, the query returns an error.            |
| `db.name`           | Optional database name.                                                                           |
| `ON CLUSTER cluster`| Runs the command across a specified cluster.                                                      |
| `SYNC`              | Makes the truncation synchronous across replicas when using replicated tables. If omitted, truncation happens asynchronously by default. |

You can use the [alter_sync](/reference/settings/session-settings/alter#alter_sync) setting to set up waiting for actions to be executed on replicas.

You can specify how long (in seconds) to wait for inactive replicas to execute `TRUNCATE` queries with the [replication_wait_for_inactive_replica_timeout](/reference/settings/session-settings/other#replication_wait_for_inactive_replica_timeout) setting.

<Note>
If the `alter_sync` is set to `2` and some replicas are not active for more than the time, specified by the `replication_wait_for_inactive_replica_timeout` setting, then an exception `UNFINISHED` is thrown.
</Note>

The `TRUNCATE TABLE` query is **not supported** for the following table engines:

- [`View`](/reference/engines/table-engines/special/view)
- [`File`](/reference/engines/table-engines/special/file)
- [`URL`](/reference/engines/table-engines/special/url)
- [`Buffer`](/reference/engines/table-engines/special/buffer)
- [`Null`](/reference/engines/table-engines/special/null)

## TRUNCATE ALL TABLES {#truncate-all-tables}
```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```
<br/>
| Parameter                  | Description                                       |
|----------------------------|---------------------------------------------------|
| `ALL`                      | Removes data from all tables in the database.     |
| `IF EXISTS`                | Prevents an error if the database does not exist. |
| `db`                       | The database name.                                |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Filters tables by pattern.           |
| `ON CLUSTER cluster`       | Runs the command across a cluster.                |

Removes all data from all tables in a database.

## TRUNCATE DATABASE {#truncate-database}
```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```
<br/>
| Parameter            | Description                                       |
|----------------------|---------------------------------------------------|
| `IF EXISTS`          | Prevents an error if the database does not exist. |
| `db`                 | The database name.                                |
| `ON CLUSTER cluster` | Runs the command across a specified cluster.      |

Removes all tables from a database but keeps the database itself. When the clause `IF EXISTS` is omitted, the query returns an error if the database does not exist.

<Note>
`TRUNCATE DATABASE` is not supported for `Replicated` databases. Instead, just `DROP` and `CREATE` the database.
</Note>
)DOCS_MD",
        .syntax = R"(
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
)",
        .related = {"DROP", "DELETE", "ALTER TABLE ... PARTITION"},
    });
}

}
