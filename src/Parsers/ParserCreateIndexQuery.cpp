#include <Parsers/ParserCreateIndexQuery.h>

#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{

bool ParserCreateIndexDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_granularity(Keyword::GRANULARITY);
    ParserToken open_p(TokenType::OpeningRoundBracket);
    ParserToken close_p(TokenType::ClosingRoundBracket);
    ParserOrderByExpressionList order_list_p;

    ParserExpressionWithOptionalArguments type_p;
    ParserExpression expression_p;
    ParserUnsignedInteger granularity_p;

    ASTPtr expr;
    ASTPtr type;
    ASTPtr granularity;

    if (open_p.ignore(pos, expected))
    {
        ASTPtr order_list;
        if (!order_list_p.parse(pos, order_list, expected))
            return false;

        if (!close_p.ignore(pos, expected))
            return false;

        if (order_list->children.empty())
            return false;

        /// CREATE INDEX with ASC, DESC is implemented only for SQL compatibility.
        /// ASC and DESC modifiers are not supported and are ignored further.
        if (order_list->children.size() == 1)
        {
            auto order_by_elem = order_list->children[0];
            expr = order_by_elem->children[0];
            /// The index's own `(...)` already groups the expression; a top-level parenthesization
            /// flag (from input like `((expr))`) is redundant and does not survive a format-parse
            /// round trip, so drop it.
            expr->setParenthesized(false);
        }
        else
        {
            auto tuple_func = makeASTOperator("tuple");

            for (const auto & order_by_elem : order_list->children)
            {
                auto elem_expr = order_by_elem->children[0];
                tuple_func->arguments->children.push_back(std::move(elem_expr));
            }
            expr = std::move(tuple_func);
        }
    }
    else if (!expression_p.parse(pos, expr, expected))
    {
        return false;
    }

    if (s_type.ignore(pos, expected))
    {
        if (!type_p.parse(pos, type, expected))
            return false;
    }

    if (s_granularity.ignore(pos, expected))
    {
        if (!granularity_p.parse(pos, granularity, expected))
            return false;
    }

    /// name is set below in ParserCreateIndexQuery
    auto index = make_intrusive<ASTIndexDeclaration>(expr, type, "");
    index->part_of_create_index_query = true;
    index->granularity = getSecondaryIndexGranularity(index->getType(), granularity);
    node = index;
    return true;
}

bool ParserCreateIndexQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTCreateIndexQuery>();
    node = query;

    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_unique(Keyword::UNIQUE);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);

    ParserIdentifier index_name_p;
    ParserCreateIndexDeclaration parser_create_idx_decl;

    ASTPtr index_name;
    ASTPtr index_decl;

    String cluster_str;
    bool if_not_exists = false;
    bool unique = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_unique.ignore(pos, expected))
        unique = true;

    if (!s_index.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!index_name_p.parse(pos, index_name, expected))
        return false;

    /// ON [db.] table_name
    if (!s_on.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    /// [ON cluster_name]
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!parser_create_idx_decl.parse(pos, index_decl, expected))
        return false;

    auto & ast_index_decl = index_decl->as<ASTIndexDeclaration &>();
    ast_index_decl.name = index_name->as<ASTIdentifier &>().name();

    query->index_name = index_name;
    query->children.push_back(index_name);

    query->index_decl = index_decl;
    query->children.push_back(index_decl);

    query->if_not_exists = if_not_exists;
    query->unique = unique;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

std::map<String, Documentation> ParserCreateIndexQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE INDEX"] =
    {
        .description = R"DOCS_MD(
Introduced in: v22.7.0

Adds a [data skipping index](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) to a table of the `MergeTree` family. It is a standalone form of [`ALTER TABLE ... ADD INDEX`](/reference/statements/alter/skipping-index#add-index) and requires the same `ALTER ADD INDEX` privilege.

## Syntax {#syntax}

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON [db.]table [ON CLUSTER cluster] expression TYPE type [GRANULARITY value]
```

- `expression` - the indexed expression, optionally in parentheses. A parenthesized list `(expr1, expr2, ...)` creates one index on all the expressions, as `INDEX name (expr1, expr2, ...)` does in `CREATE TABLE`.
- `TYPE type [GRANULARITY value]` - as in the `INDEX` clause of `CREATE TABLE`, see [skip index types](/reference/engines/table-engines/mergetree-family/mergetree#skip-index-types).
- `IF NOT EXISTS` - if the table already has an index named `name`, the query does nothing.

Like `ADD INDEX`, the statement changes only the table metadata. To build the index for the data already in the table, run [`ALTER TABLE ... MATERIALIZE INDEX`](/reference/statements/alter/skipping-index#materialize-index).

## SQL compatibility {#sql-compatibility}

For compatibility with other database systems:

- `ASC` and `DESC` after the expressions of a parenthesized list are ignored.
- `CREATE UNIQUE INDEX` is rejected unless [`create_index_ignore_unique`](/reference/settings/session-settings/create#create_index_ignore_unique) is `1`. Then `UNIQUE` is ignored and an ordinary index is created.
- `CREATE INDEX` without `TYPE` is rejected unless [`allow_create_index_without_type`](/reference/settings/session-settings/allow#allow_create_index_without_type) is `1`. Then the query does nothing.

## Example {#example}

```sql title="Query"
CREATE TABLE t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
CREATE INDEX idx_s ON t (s) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t MATERIALIZE INDEX idx_s SETTINGS mutations_sync = 1;
SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't';
```

```text title="Response"
┌─name──┬─type─────────┬─expr─┬─granularity─┐
│ idx_s │ bloom_filter │ s    │           1 │
└───────┴──────────────┴──────┴─────────────┘
```

## Related content {#related-content}

- [`ALTER TABLE ... INDEX`](/reference/statements/alter/skipping-index)
- [`DROP INDEX`](/reference/statements/drop#drop-index)
- [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices)
)DOCS_MD",
        .syntax = R"(
CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON [db.]table [ON CLUSTER cluster] expression TYPE type [GRANULARITY value]
)",
        .parent = "CREATE",
        .related = {"ALTER TABLE ... INDEX", "CREATE TABLE", "DROP"},
    };

    return documentation;
}

}
