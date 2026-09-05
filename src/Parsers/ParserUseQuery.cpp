#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use(Keyword::USE);
    ParserKeyword s_database(Keyword::DATABASE);
    /// `USE a.b` selects the database `a.b`, or the tables `b.*` of the database `a` (a hierarchical name).
    ParserCompoundIdentifier name_p{/*table_name_with_optional_uuid*/ false, /*allow_query_parameter*/ true};

    if (!s_use.ignore(pos, expected))
        return false;

    ASTPtr database;
    Expected test_expected;

    /// test if we have DATABASE <identifier> pattern without moving pos
    Pos test_pos = pos;
    ASTPtr test_node;

    bool has_database_keyword_pattern =
        s_database.parse(test_pos, test_node, test_expected) &&
        name_p.parse(test_pos, test_node, test_expected);

    // now the actual parsing
    if (has_database_keyword_pattern)
    {
        // Parse DATABASE <identifier>
        s_database.ignore(pos, expected);
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else
    {
        // Parse identifier directly (handles "USE database" where database is a name)
        if (!name_p.parse(pos, database, expected))
            return false;
    }

    auto query = make_intrusive<ASTUseQuery>();
    query->set(query->database, database);
    node = query;

    return true;
}

}

namespace DB
{

void registerStatementUse(StatementFactory & factory)
{
    factory.registerStatement("USE",
    {
        .description = R"DOCS_MD(
```sql
USE [DATABASE] db
```

Lets you set the current database for the session.

The current database is used for searching for tables if the database is not explicitly defined in the query with a dot before the table name.

This query can't be made when using the HTTP protocol, since there is no concept of a session.

## Hierarchical names {#hierarchical-names}

Database and table names can contain dots, and such names can be written without quotes. A qualified name like `a.b.c` has no fixed split into a database and a table: it is resolved against the existing databases and tables, trying the database `a.b` with the table `c`, then the database `a` with the table `b.c`, and then the table `a.b.c` of the current database. Quoting does not matter: `a."b.c"`, `"a.b".c` and `a.b.c` are the same name. This is how the tables of a data lake catalog, named `namespace.table`, are addressed: `SELECT * FROM catalog.namespace.table`.

The current database can be hierarchical as well. `USE a.b` is allowed when the database `a.b` exists, when the database `a` has tables named `b.*` (then `b` is a namespace of tables, and `SELECT * FROM c` reads the table `a`.`b.c`), or when there are databases named `a.b.*` (then `SELECT * FROM c.d` reads the table `d` of the database `a.b.c`). `SHOW TABLES` lists the tables under the selected name, with the names relative to it.

The unquoted parts of a name never create a new namespace: `CREATE TABLE a.b.c` creates the table `a`.`b.c` only if the database `a` already has tables named `b.*`, which protects against typos in database names. The first table of a namespace is created by quoting its name: `CREATE TABLE a."b.c"`.

```sql
CREATE DATABASE catalog;
CREATE TABLE catalog."sales.orders" (id UInt64) ENGINE = MergeTree ORDER BY id;

SELECT * FROM catalog.sales.orders;

USE catalog;
SELECT * FROM sales.orders;
CREATE TABLE sales.customers (id UInt64) ENGINE = MergeTree ORDER BY id; -- creates the table `sales.customers`

USE catalog.sales;
SELECT * FROM orders;
SHOW TABLES; -- customers, orders
```
)DOCS_MD",
        .syntax = R"(
USE [DATABASE] db
)",
        .related = {"CREATE DATABASE", "SHOW", "SET"},
    });
}

}
