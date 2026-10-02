#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserTablePropertiesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_exists(Keyword::EXISTS);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_dictionary(Keyword::DICTIONARY);
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p(true);

    ASTPtr database;
    ASTPtr table;
    boost::intrusive_ptr<ASTQueryWithTableAndOutput> query;

    bool parse_only_database_name = false;
    bool parse_show_create_view = false;
    bool exists_view = false;

    bool temporary = false;

    if (s_exists.ignore(pos, expected))
    {
        if (s_database.ignore(pos, expected))
        {
            query = make_intrusive<ASTExistsDatabaseQuery>();
            parse_only_database_name = true;
        }
        else
        {
            if (s_temporary.ignore(pos, expected))
                temporary = true;

            if (s_view.ignore(pos, expected))
            {
                query = make_intrusive<ASTExistsViewQuery>();
                exists_view = true;
            }
            else if (s_table.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTExistsTableQuery>();
            else if (s_dictionary.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTExistsDictionaryQuery>();
            else
                query = make_intrusive<ASTExistsTableQuery>();
        }
    }
    else if (s_show.ignore(pos, expected))
    {
        bool has_create = false;

        if (s_create.checkWithoutMoving(pos, expected))
        {
            has_create = true;
            s_create.ignore(pos, expected);
        }

        // Check for TEMPORARY keyword after SHOW [CREATE]
        if (s_temporary.ignore(pos, expected))
            temporary = true;

        if (s_database.ignore(pos, expected))
        {
            parse_only_database_name = true;
            query = make_intrusive<ASTShowCreateDatabaseQuery>();
        }
        else if (s_dictionary.checkWithoutMoving(pos, expected))
            query = make_intrusive<ASTShowCreateDictionaryQuery>();
        else if (s_view.ignore(pos, expected))
        {
            query = make_intrusive<ASTShowCreateViewQuery>();
            parse_show_create_view = true;
        }
        else
        {
            /// We support `SHOW CREATE tbl;` and `SHOW TABLE tbl`,
            /// but do not support `SHOW tbl`, which is ambiguous
            /// with other statement like `SHOW PRIVILEGES`.
            if (has_create || s_table.checkWithoutMoving(pos, expected))
                query = make_intrusive<ASTShowCreateTableQuery>();
            else
                return false;
        }
    }
    else
    {
        return false;
    }
    if (parse_only_database_name)
    {
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else
    {
        if (!(exists_view || parse_show_create_view))
        {
            if (temporary || s_temporary.ignore(pos, expected))
                query->setIsTemporary(true);

            if (!s_table.ignore(pos, expected))
                s_dictionary.ignore(pos, expected);
        }

        query->setIsTemporary(temporary);

        if (!name_p.parse(pos, table, expected))
            return false;
        if (s_dot.ignore(pos, expected))
        {
            database = table;
            if (!name_p.parse(pos, table, expected))
                return false;
        }
    }

    query->database = database;
    query->table = table;

    if (database)
        query->children.push_back(database);

    if (table)
        query->children.push_back(table);

    node = query;

    return true;
}


}

namespace DB
{

void registerStatementExists(StatementFactory & factory)
{
    factory.registerStatement("EXISTS",
    {
        .description = R"DOCS_MD(
```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `UInt8`-type column, which contains the single value `0` if the table or database does not exist, or `1` if the table exists in the specified database.

## Subquery form {#subquery-form}

The `EXISTS` operator checks whether a subquery returns any rows. It returns `0` if the subquery result is empty; otherwise, it returns `1`.

You can use `EXISTS` in a [`WHERE`](/reference/statements/select/where) clause. The subquery cannot reference tables or columns from the outer query.

**Syntax**

```sql
EXISTS(subquery)
```

**Examples**

Check whether a subquery returns rows:

```sql title="Query"
SELECT
    EXISTS(SELECT * FROM numbers(10) WHERE number > 8),
    EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

Use `EXISTS` in a `WHERE` clause with a subquery that returns several rows:

```sql title="Query"
SELECT count()
FROM numbers(10)
WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8)
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

When the subquery result is empty, `EXISTS` returns `0`:

```sql title="Query"
SELECT count()
FROM numbers(10)
WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```
)DOCS_MD",
        .syntax = R"(
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
)",
        .related = {"SHOW", "DESCRIBE TABLE", "CREATE"},
    });
}

}
