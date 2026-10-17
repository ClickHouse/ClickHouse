#include <Parsers/ParserParallelWithQuery.h>

#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

ParserParallelWithQuery::ParserParallelWithQuery(IParser & subquery_parser_, ASTPtr first_subquery_)
    : subquery_parser(subquery_parser_), first_subquery(first_subquery_)
{
}


bool ParserParallelWithQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword keyword_parallel_with{Keyword::PARALLEL_WITH};

    auto old_pos = pos;
    if (!keyword_parallel_with.ignore(pos, expected))
        return true;

    ASTs subqueries;
    subqueries.push_back(first_subquery);

    do
    {
        ASTPtr subquery;
        if (!subquery_parser.parse(pos, subquery, expected))
        {
            pos = old_pos;
            break;
        }
        subqueries.push_back(subquery);
        old_pos = pos;
    } while (keyword_parallel_with.ignore(pos, expected));

    auto res = make_intrusive<ASTParallelWithQuery>();
    res->children = std::move(subqueries);
    node = res;

    return true;
}

std::map<String, Documentation> ParserParallelWithQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["PARALLEL WITH"] =
    {
        .description = R"DOCS_MD(
Allows to execute multiple statements in parallel.

## Syntax {#syntax}

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Executes statements `statement1`, `statement2`, `statement3`, ... in parallel with each other. Only statements that neither return a result set nor expect input data can be combined, see [Restrictions](#restrictions).

Executing statements in parallel may be faster than just a sequence of the same statements in many cases. For example, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` is likely to be faster than `statement1; statement2; statement3`.

## Examples {#examples}

Creates two tables in parallel:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Drops two tables in parallel:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

## Restrictions {#restrictions}

A statement that returns a result set, such as `SELECT`, or expects input data is rejected with the `INCORRECT_QUERY` error.

DDL executed [`ON CLUSTER`](/reference/statements/distributed-ddl) or through the DDL log of a [`Replicated`](/reference/engines/database-engines/replicated) database returns a status table, so it is rejected too, but only after it has been sent to the hosts, which still execute it. It can be combined if [`distributed_ddl_output_mode`](/reference/settings/session-settings/distributed-ddl#distributed_ddl_output_mode) is `none` or `none_only_active`:

```sql
SET distributed_ddl_output_mode = 'none';

CREATE TABLE table1 ON CLUSTER default (x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2 ON CLUSTER default (y String) ENGINE = MergeTree ORDER BY tuple();
```

## Settings {#settings}

Setting [max_threads](/reference/settings/session-settings/max-threads#max_threads) controls how many threads are spawned.

## Comparison with UNION {#comparison-with-union}

The `PARALLEL WITH` clause is a bit similar to [UNION](/reference/statements/select/union), which also executes its operands in parallel. However there are some differences:
- `PARALLEL WITH` doesn't return any results from executing its operands, it can only rethrow an exception from them if any;
- `PARALLEL WITH` combines statements that neither return a result set nor expect input data, while `UNION` combines `SELECT` queries.
)DOCS_MD",
        .syntax = R"(
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
)",
        .related = {"CREATE", "DROP", "SELECT"},
    };

    return documentation;
}

}
