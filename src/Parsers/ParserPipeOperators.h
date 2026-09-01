#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** A chain of pipe operators that can follow any SELECT query and apply further transformations to its result:
  *
  *     FROM t |> WHERE x > 1 |> AGGREGATE count() AS c GROUP BY y |> ORDER BY c DESC |> LIMIT 10
  *
  * Each pipe operator wraps the query parsed so far into a subquery in the FROM clause of a new SELECT query,
  * so the resulting AST is the same as the AST of the equivalent query written with nested subqueries.
  * Inside every operator, the regular ClickHouse syntax is used.
  *
  * `query` is the query parsed so far (ASTSelectWithUnionQuery), and `pos` must point to the first `|>` token.
  * On success, `query` is replaced with the transformed query.
  */
bool parsePipeOperators(IParser::Pos & pos, ASTPtr & query, Expected & expected);

}
