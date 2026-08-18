#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/StatementFactory.h>


namespace DB
{

bool ParserSelectWithUnionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list_node;
    ParserUnionList parser;

    if (!parser.parse(pos, list_node, expected))
        return false;

    /// NOTE: We can't simply flatten inner union query now, since we may have different union mode in query,
    /// so flatten may change it's semantics. For example:
    /// flatten `SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1)` -> `SELECT 1 UNION SELECT 1 UNION ALL SELECT 1`

    /// If we got only one child which is ASTSelectWithUnionQuery, just lift it up
    auto & expr_list = list_node->as<ASTExpressionList &>();
    if (expr_list.children.size() == 1 && expr_list.children.at(0)->as<ASTSelectWithUnionQuery>())
    {
        node = std::move(expr_list.children.at(0));
    }
    else
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();

        node = select_with_union_query;
        select_with_union_query->list_of_selects = list_node;
        select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
        select_with_union_query->list_of_modes = parser.getUnionModes();
    }

    /// The query can be followed by a chain of pipe operators, e.g.: FROM t |> WHERE x |> LIMIT 1.
    if (pos->type == TokenType::PipeOperator)
        return parsePipeOperators(pos, node, expected);

    return true;
}

}

namespace DB
{

REGISTER_STATEMENTS(Union)
{
    factory.registerStatement("UNION", "SELECT",
    {
        .description = R"(
Combines the results of several queries. The queries must produce the same number of columns, in the same order and of
compatible types. `UNION DISTINCT` removes duplicate rows from the result of the union, whereas `UNION ALL` keeps
them. If neither `ALL` nor `DISTINCT` is specified, the behaviour depends on the setting `union_default_mode`.
)",
        .syntax = R"(
SELECT ... UNION [ALL | DISTINCT] SELECT ... [UNION [ALL | DISTINCT] SELECT ...]
)",
        .examples = {{"Concatenate the results of two queries", R"(
SELECT 1 AS x
UNION ALL
SELECT 2 AS x;
)", ""}},
        .related = {"SELECT", "INTERSECT", "EXCEPT", "DISTINCT", "JOIN"},
    });

    factory.registerStatement("INTERSECT", "SELECT",
    {
        .description = R"(
Returns only the rows which result from both the first and the second query. The queries must produce the same number
of columns, in the same order and of compatible types. The result can contain duplicate rows; use `INTERSECT DISTINCT`
if this is not desirable. `INTERSECT` has a higher precedence than `UNION` and `EXCEPT`.
)",
        .syntax = R"(
SELECT column1 [, column2] FROM table1 [WHERE condition]
INTERSECT [ALL | DISTINCT]
SELECT column1 [, column2] FROM table2 [WHERE condition]
)",
        .examples = {{"Intersect the results of two queries", R"(
SELECT number FROM numbers(10)
INTERSECT
SELECT number FROM numbers(5);
)", ""}},
        .related = {"SELECT", "UNION", "EXCEPT", "IN"},
    });

    factory.registerStatement("EXCEPT", "SELECT",
    {
        .description = R"(
Returns only the rows which result from the first query without the second. The queries must produce the same number
of columns, in the same order and of compatible types. The result can contain duplicate rows; use `EXCEPT DISTINCT` if
this is not desirable.
)",
        .syntax = R"(
SELECT column1 [, column2] FROM table1 [WHERE condition]
EXCEPT [ALL | DISTINCT]
SELECT column1 [, column2] FROM table2 [WHERE condition]
)",
        .examples = {{"Subtract the result of one query from another", R"(
SELECT number FROM numbers(10)
EXCEPT
SELECT number FROM numbers(5);
)", ""}},
        .related = {"SELECT", "UNION", "INTERSECT", "EXCEPT modifier"},
    });
}

}
