#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


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

std::map<String, Documentation> ParserKillQueryQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["KILL"] =
    {
        .description = R"DOCS_MD(
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

This page describes the `KILL QUERY`, `KILL MUTATION`, `KILL TRANSACTION` and `KILL PART_MOVE_TO_SHARD` statements.

By default, `KILL QUERY` throws a `NOTHING_TO_KILL` exception when its `WHERE` expression leaves no eligible rows in `system.processes` after excluding the current `KILL` statement, and `KILL MUTATION` throws when its `WHERE` expression matches zero rows in `system.mutations`. To allow empty matches without an exception, set [`kill_throw_if_noop`](/operations/settings/settings#kill_throw_if_noop) to `false`.
`KILL TRANSACTION` and `KILL PART_MOVE_TO_SHARD` never throw for empty matches: they return an empty result.
`ON CLUSTER` execution does not throw for empty matches because match results are not aggregated across hosts.

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

A user who has not been granted `SELECT` on `system.processes` can still cancel their own query by naming it:
`KILL QUERY WHERE query_id = '<id>'`. For a user who holds neither that grant nor `KILL QUERY`, an id that is
not running as them cancels nothing and throws a `NOTHING_TO_KILL` exception by default. With
[`kill_throw_if_noop`](/operations/settings/settings#kill_throw_if_noop) set to `false`, it returns no rows instead;
a user who holds `KILL QUERY` but not that `SELECT` is refused for want of the `SELECT`.
Any other `WHERE` condition, and `ON CLUSTER`, keep requiring the grants described above.

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

## KILL TRANSACTION {#kill-transaction}

Introduced in: v22.4.0

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

<Note>
This statement is part of an experimental feature set.
Enable experimental transaction support by adding this setting to your [configuration](/concepts/features/configuration/server-config/configuration-files):

```xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

For more information see the page [Transactional (ACID) support](/concepts/features/operations/insert/transactions#transactions-commit-and-rollback).
</Note>

```sql
KILL TRANSACTION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.transactions query>
  [TEST]
  [FORMAT format]
```

Rolls back the running [transactions](/reference/statements/transactions) selected from the [`system.transactions`](/reference/system-tables/transactions) table by the `WHERE` clause.
The session of a killed transaction is then in the same state as after a failed query: until it runs `ROLLBACK`, `COMMIT` and its other queries throw an `INVALID_TRANSACTION` exception, see [`COMMIT`](/reference/statements/transactions#commit).

The statement requires the `KILL TRANSACTION` privilege and `SELECT` on `system.transactions`.
The response contains the `kill_status` column, which is `waiting` for a transaction that was rolled back, followed by the `tid`, `tid_hash`, `elapsed`, `is_readonly` and `state` columns of `system.transactions`.

A test query (`TEST`) only checks the user's rights and displays a list of transactions to roll back.

Example:

```sql title="Query"
KILL TRANSACTION WHERE tid_hash = 17701405580889899494 FORMAT Vertical
```

```text title="Response"
Row 1:
──────
kill_status: waiting
tid:         (105,84,'42343455-b083-479e-b5d5-d50cf2ec2f58',3)
tid_hash:    17701405580889899494
elapsed:     0.160418317
is_readonly: 0
state:       RUNNING
```

## KILL PART_MOVE_TO_SHARD {#kill-part-move-to-shard}

Introduced in: v21.11.0

<ExperimentalBadge/>

<Note>
Moving parts between shards is an experimental feature, enabled per table by the MergeTree setting [`part_moves_between_shards_enable`](/reference/settings/merge-tree-settings/part-moves#part_moves_between_shards_enable).
</Note>

```sql
KILL PART_MOVE_TO_SHARD [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.part_moves_between_shards query>
  [TEST]
  [FORMAT format]
```

Cancels the moves of parts between shards, started by `ALTER TABLE ... MOVE PART ... TO SHARD`, that are selected from the [`system.part_moves_between_shards`](/reference/system-tables/part_moves_between_shards) table by the `WHERE` clause.
A cancelled move is rolled back in the background, and its `state` in `system.part_moves_between_shards` becomes `CANCELLED`.
A move that has reached the `SOURCE_DROP` state can no longer be cancelled and runs to completion.

The statement requires `SELECT` on `system.part_moves_between_shards` and, to cancel a move, the `ALTER MOVE PARTITION` privilege on its table and the `MOVE PARTITION BETWEEN SHARDS` privilege.
The response contains the `kill_status` column, which is `waiting` for a cancelled move and `cant_cancel` for a move that can no longer be cancelled, followed by the `database`, `table`, `task_name`, `task_uuid`, `part_name`, `to_shard` and `state` columns of `system.part_moves_between_shards`.

A test query (`TEST`) only displays a list of moves to cancel. The `SYNC` modifier is not supported and throws a `NOT_IMPLEMENTED` exception.

Example:

```sql title="Query"
KILL PART_MOVE_TO_SHARD WHERE database = 'default' AND table = 'src' FORMAT Vertical
```

```text title="Response"
Row 1:
──────
kill_status: waiting
database:    default
table:       src
task_name:   task-0000000000
task_uuid:   7e073eb5-be5a-46a9-9538-ea176bd7ebe8
part_name:   all_0_0_0
to_shard:    /clickhouse/tables/s2
state:       SYNC_SOURCE
```
)DOCS_MD",
        .syntax = R"(
KILL QUERY [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.processes query> [SYNC|ASYNC|TEST] [FORMAT format]
KILL MUTATION [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.mutations query> [TEST] [FORMAT format]
KILL TRANSACTION [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.transactions query> [TEST] [FORMAT format]
KILL PART_MOVE_TO_SHARD [ON CLUSTER cluster] WHERE <where expression to SELECT FROM system.part_moves_between_shards query> [TEST] [FORMAT format]
)",
        .related = {"SYSTEM", "SHOW", "ALTER", "BEGIN TRANSACTION"},
    };

    return documentation;
}

}
