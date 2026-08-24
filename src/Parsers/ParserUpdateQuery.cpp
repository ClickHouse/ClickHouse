#include <Parsers/ParserUpdateQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

namespace DB
{

bool ParserUpdateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = make_intrusive<ASTUpdateQuery>();
    node = query;

    ParserKeyword s_update(Keyword::UPDATE);
    ParserKeyword s_set(Keyword::SET);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_on{Keyword::ON};
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_in_partition(Keyword::IN_PARTITION);

    ParserExpression parser_exp_elem;
    ParserPartition parser_partition;

    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(),
        std::make_unique<ParserToken>(TokenType::Comma));

    if (!s_update.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    if (s_on.ignore(pos, expected))
    {
        String cluster_str;
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        query->cluster = cluster_str;
    }

    if (!s_set.ignore(pos, expected))
        return false;

    if (!parser_assignment_list.parse(pos, query->assignments, expected))
        return false;

    if (s_in_partition.ignore(pos, expected))
    {
        if (!parser_partition.parse(pos, query->partition, expected))
            return false;
    }

    if (!s_where.ignore(pos, expected))
        return false;

    if (!parser_exp_elem.parse(pos, query->predicate, expected))
        return false;

    /// ParserExpression, in contrast to ParserExpressionWithOptionalAlias,
    /// does not expect an alias after the expression. However, in certain cases,
    /// it uses ParserExpressionWithOptionalAlias recursively, and use its result.
    /// This is the case when it parses a single expression in parentheses, e.g.,
    /// it does not allow
    /// 1 AS x
    /// but it can parse
    /// (1 AS x)
    /// which we should not allow as well.
    if (!query->predicate->tryGetAlias().empty())
        return false;

    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, query->settings_ast, expected))
            return false;
    }

    auto add_to_children = [&](const auto & ast)
    {
        if (ast)
            query->children.push_back(ast);
    };

    add_to_children(query->database);
    add_to_children(query->table);
    add_to_children(query->partition);
    add_to_children(query->predicate);
    add_to_children(query->assignments);
    add_to_children(query->settings_ast);

    return true;
}

}

namespace DB
{

void registerStatementUpdate(StatementFactory & factory)
{
    factory.registerStatement("UPDATE",
    {
        .description = R"(
Updates the rows matching the filter expression in a table.

It is called a lightweight `UPDATE` to contrast it with `ALTER TABLE ... UPDATE`: the new values are written into patch
parts and applied on the fly when the data is read, whereas the affected data parts are rewritten later by merges or by
`ALTER TABLE ... APPLY PATCHES`.

**Examples**

**Update the rows matching a condition**

```sql title="Query"
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();
```
)",
        .syntax = R"(
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
)",
        .related = {"ALTER TABLE ... UPDATE", "ALTER TABLE ... APPLY PATCHES", "DELETE", "INSERT INTO"},
    });
}

}
