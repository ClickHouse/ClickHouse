---
slug: /en/sql-reference/statements/kill
sidebar_position: 46
sidebar_label: KILL
title: "KILL Statements"
---

There are two kinds of kill statements: to kill a query and to kill a mutation

## KILL QUERY

``` sql
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
``` sql
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
``` sql
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
``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

Read-only users can only stop their own queries.

By default, the asynchronous version of queries is used (`ASYNC`), which does not wait for confirmation that queries have stopped.

The synchronous version (`SYNC`) waits for all queries to stop and displays information about each process as it stops.
The response contains the `kill_status` column, which can take the following values:

1.  `finished` – The query was terminated successfully.
2.  `waiting` – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can’t be stopped.

A test query (`TEST`) only checks the user’s rights and displays a list of queries to stop.

## KILL MUTATION

One of the first things to check if a ClickHouse system or service is not running well is for long-running, incomplete mutations. The asynchronous (background) nature of mutations can cause a large queue of them that can then consume all available resources on the service. You may need to either pause all new mutations, INSERTs, and SELECTs and allow the queue of mutations to complete, or else manually kill some of these mutations.

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tries to cancel and remove [mutations](../../sql-reference/statements/alter/index.md#alter-mutations) that are currently executing. Mutations to cancel are selected from the [`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations) table using the filter specified by the `WHERE` clause of the `KILL` query.

A test query (`TEST`) only checks the user’s rights and displays a list of mutations to stop.

Examples:

Get a count() of the number of incomplete mutations:

Count of mutations from a single ClickHouse node:
``` sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Count of mutations from a ClickHouse cluster of replicas:
``` sql
SELECT count(*)
FROM clusterAllReplicas('default',system.mutations)
WHERE is_done = 0;
```

Query the list of incomplete mutations:

List of mutations from a single ClickHouse node:
``` sql
SELECT mutation_id,*
FROM system.mutations
WHERE is_done = 0;
```

List of mutations from a ClickHouse cluster:
``` sql
SELECT mutation_id,*
FROM clusterAllReplicas('default',system.mutations)
WHERE is_done = 0;
```

Kill the mutations as needed:
``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Changes already made by the mutation are not rolled back.
