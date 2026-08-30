#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String cluster_str;
    auto query = make_intrusive<ASTKillQueryQuery>();

    ParserKeyword p_kill{Keyword::KILL};
    ParserKeyword p_query{Keyword::QUERY};
    ParserKeyword p_mutation{Keyword::MUTATION};
    ParserKeyword p_part_move_to_shard{Keyword::PART_MOVE_TO_SHARD};
    ParserKeyword p_transaction{Keyword::TRANSACTION};
    ParserKeyword p_on{Keyword::ON};
    ParserKeyword p_test{Keyword::TEST};
    ParserKeyword p_sync{Keyword::SYNC};
    ParserKeyword p_async{Keyword::ASYNC};
    ParserKeyword p_where{Keyword::WHERE};
    ParserExpression p_where_expression;

    if (!p_kill.ignore(pos, expected))
        return false;

    if (p_query.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Query;
    else if (p_mutation.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Mutation;
    else if (p_part_move_to_shard.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::PartMoveToShard;
    else if (p_transaction.ignore(pos, expected))
        query->type = ASTKillQueryQuery::Type::Transaction;
    else
        return false;

    if (p_on.ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
        return false;

    if (!p_where.ignore(pos, expected) || !p_where_expression.parse(pos, query->where_expression, expected))
        return false;

    if (p_sync.ignore(pos, expected))
        query->sync = true;
    else if (p_async.ignore(pos, expected))
        query->sync = false;
    else if (p_test.ignore(pos, expected))
        query->test = true;

    query->cluster = cluster_str;
    query->children.emplace_back(query->where_expression);
    node = std::move(query);
    return true;
}

}

namespace DB
{

void registerStatementKillQuery(StatementFactory & factory)
{
    factory.registerStatement("KILL",
    {
        .description = R"DOCS_MD(
There are two kinds of kill statements: to kill a query and to kill a mutation

## KILL QUERY {#kill-query}

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Attempts to forcibly terminate the currently running queries.
The queries to terminate are selected from the system.processes table using the criteria defined in the `WHERE` clause of the `KILL` query.

Examples:

First, you'll need to get the list of incomplete queries. This SQL query provides them according to those running the longest:

List from a single ClickHouse node:
```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

List from a ClickHouse cluster:
```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Kill the query:
```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

<Tip>
If you are killing a query in ClickHouse Cloud or in a self-managed cluster, then be sure to use the ```ON CLUSTER [cluster-name]```option, in order to ensure the query is killed on all replicas
</Tip>

Read-only users can only stop their own queries.

By default, the asynchronous version of queries is used (`ASYNC`), which does not wait for confirmation that queries have stopped.

The synchronous version (`SYNC`) waits for all queries to stop and displays information about each process as it stops.
The response contains the `kill_status` column, which can take the following values:

1.  `finished` – The query was terminated successfully.
2.  `waiting` – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

A test query (`TEST`) only checks the user's rights and displays a list of queries to stop.

## KILL MUTATION {#kill-mutation}

The presence of long-running or incomplete mutations often indicates that a ClickHouse service is running poorly. The asynchronous nature of mutations can cause them to consume all available resources on a system. You may need to either:

- Pause all new mutations, `INSERT`s , and `SELECT`s and allow the queue of mutations to complete.
- Or manually kill some of these mutations by sending a `KILL` command.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tries to cancel and remove [mutations](/reference/statements/alter/index#mutations) that are currently executing. Mutations to cancel are selected from the [`system.mutations`](/reference/system-tables/mutations) table using the filter specified by the `WHERE` clause of the `KILL` query.

A test query (`TEST`) only checks the user's rights and displays a list of mutations to stop.

Examples:

Get a `count()` of the number of incomplete mutations:

Count of mutations from a single ClickHouse node:
```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Count of mutations from a ClickHouse cluster of replicas:
```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Query the list of incomplete mutations:

List of mutations from a single ClickHouse node:
```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

List of mutations from a ClickHouse cluster:
```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Kill the mutations as needed:
```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Changes already made by the mutation are not rolled back.

<Note>
`is_killed=1` column (ClickHouse Cloud only) in the [system.mutations](/reference/system-tables/mutations) table does not necessarily mean the mutation is completely finalized. It is possible for a mutation to remain in a state where `is_killed=1` and `is_done=0` for an extended period. This can happen if another long-running mutation is blocking the killed mutation. This is a normal situation.
</Note>
)DOCS_MD",
        .syntax = R"(
KILL QUERY [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.processes query> [SYNC|ASYNC|TEST] [FORMAT format]
KILL MUTATION [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.mutations query> [TEST] [FORMAT format]
)",
        .related = {"SYSTEM", "SHOW", "ALTER"},
    });
}

}
