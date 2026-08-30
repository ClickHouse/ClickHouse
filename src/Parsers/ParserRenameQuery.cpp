#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTRenameQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserRenameQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_rename(Keyword::RENAME);
    ParserKeyword s_rename_table(Keyword::RENAME_TABLE);
    ParserKeyword s_exchange_tables(Keyword::EXCHANGE_TABLES);
    ParserKeyword s_rename_dictionary(Keyword::RENAME_DICTIONARY);
    ParserKeyword s_exchange_dictionaries(Keyword::EXCHANGE_DICTIONARIES);
    ParserKeyword s_rename_database(Keyword::RENAME_DATABASE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_and(Keyword::AND);
    ParserToken s_comma(TokenType::Comma);

    bool exchange = false;
    bool dictionary = false;

    if (s_rename_database.ignore(pos, expected))
    {
        ASTPtr from_db;
        ASTPtr to_db;
        ParserIdentifier db_name_p(true);
        bool if_exists = s_if_exists.ignore(pos, expected);
        if (!db_name_p.parse(pos, from_db, expected))
            return false;
        if (!s_to.ignore(pos, expected))
            return false;
        if (!db_name_p.parse(pos, to_db, expected))
            return false;

        String cluster_str;
        if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
        {
            if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
                return false;
        }
        ASTRenameQuery::Elements rename_elements;
        rename_elements.emplace_back();
        rename_elements.back().if_exists = if_exists;
        rename_elements.back().from.database = from_db;
        rename_elements.back().to.database = to_db;

        auto query = make_intrusive<ASTRenameQuery>(std::move(rename_elements));
        query->database = true;
        query->cluster = cluster_str;
        node = query;
        return true;
    }
    if (s_rename_table.ignore(pos, expected))
        ;
    else if (s_exchange_tables.ignore(pos, expected))
        exchange = true;
    else if (s_rename_dictionary.ignore(pos, expected))
        dictionary = true;
    else if (s_exchange_dictionaries.ignore(pos, expected))
    {
        exchange = true;
        dictionary = true;
    }
    else if (s_rename.ignore(pos, expected))
        ;
    else
        return false;

    const auto ignore_delim = [&] { return exchange ? s_and.ignore(pos, expected) : s_to.ignore(pos, expected); };

    ASTRenameQuery::Elements elements;

    while (true)
    {
        if (!elements.empty() && !s_comma.ignore(pos))
            break;

        ASTRenameQuery::Element & ref = elements.emplace_back();

        if (!exchange)
            ref.if_exists = s_if_exists.ignore(pos, expected);

        if (!parseDatabaseAndTableAsAST(pos, expected, ref.from.database, ref.from.table)
            || !ignore_delim()
            || !parseDatabaseAndTableAsAST(pos, expected, ref.to.database, ref.to.table))
            return false;
    }

    String cluster_str;
    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTRenameQuery>(std::move(elements));
    query->cluster = cluster_str;
    query->exchange = exchange;
    query->dictionary = dictionary;
    node = query;
    return true;
}


}

namespace DB
{

void registerStatementRename(StatementFactory & factory)
{
    factory.registerStatement("RENAME",
    {
        .description = R"DOCS_MD(
Renames databases, tables, or dictionaries. Several entities can be renamed in a single query.
Note that the `RENAME` query with several entities is non-atomic operation. To swap entities names atomically, use the [EXCHANGE](/reference/statements/exchange) statement.

**Syntax**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

## RENAME DATABASE {#rename-database}

Renames databases.

**Syntax**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

## RENAME TABLE {#rename-table}

Renames one or more tables.

Renaming tables is a light operation. If you pass a different database after `TO`, the table will be moved to this database. However, the directories with databases must reside in the same file system. Otherwise, an error is returned.
If you rename multiple tables in one query, the operation is not atomic. It may be partially executed, and queries in other sessions may get `Table ... does not exist ...` error.

**Syntax**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Example**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

And you can use a simpler sql:
```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

## RENAME DICTIONARY {#rename-dictionary}

Renames one or several dictionaries. This query can be used to move dictionaries between databases.

**Syntax**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**See Also**

- [Dictionaries](/reference/statements/create/dictionary)
)DOCS_MD",
        .syntax = R"(
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
)",
        .related = {"EXCHANGE", "CREATE", "ALTER"},
    });

    factory.registerStatement("EXCHANGE",
    {
        .description = R"DOCS_MD(
Exchanges the names of two tables or dictionaries atomically.
This task can also be accomplished with a [`RENAME`](/reference/statements/rename) query using a temporary name, but the operation is not atomic in that case.

<Note>
The `EXCHANGE` query is supported by the [`Atomic`](/reference/engines/database-engines/atomic) and [`Shared`](/products/cloud/features/infrastructure/shared-catalog#shared-database-engine) database engines only.
</Note>

**Syntax**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

## EXCHANGE TABLES {#exchange-tables}

Exchanges the names of two tables.

**Syntax**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

### EXCHANGE MULTIPLE TABLES {#exchange-multiple-tables}

You can exchange multiple table pairs in a single query by separating them with commas.

<Note>
When exchanging multiple table pairs, the exchanges are performed **sequentially, not atomically**. If an error occurs during the operation, some table pairs may have been exchanged while others have not.
</Note>

**Example**

```sql title="Query"
-- Create tables
CREATE TABLE a (a UInt8) ENGINE=Memory;
CREATE TABLE b (b UInt8) ENGINE=Memory;
CREATE TABLE c (c UInt8) ENGINE=Memory;
CREATE TABLE d (d UInt8) ENGINE=Memory;

-- Exchange two pairs of tables in one query
EXCHANGE TABLES a AND b, c AND d;

SHOW TABLE a;
SHOW TABLE b;
SHOW TABLE c;
SHOW TABLE d;
```

```sql title="Response"
-- Now table 'a' has the structure of 'b', and table 'b' has the structure of 'a'
┌─statement──────────────┐
│ CREATE TABLE default.a↴│
│↳(                     ↴│
│↳    `b` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.b↴│
│↳(                     ↴│
│↳    `a` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘

-- Now table 'c' has the structure of 'd', and table 'd' has the structure of 'c'
┌─statement──────────────┐
│ CREATE TABLE default.c↴│
│↳(                     ↴│
│↳    `d` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.d↴│
│↳(                     ↴│
│↳    `c` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
```

## EXCHANGE DICTIONARIES {#exchange-dictionaries}

Exchanges the names of two dictionaries.

**Syntax**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**See Also**

- [Dictionaries](/reference/statements/create/dictionary)
)DOCS_MD",
        .syntax = R"(
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
)",
        .related = {"RENAME", "REPLACE TABLE", "CREATE DATABASE"},
    });
}

}
