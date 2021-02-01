---
toc_priority: 46
toc_title: KILL
---

# KILL Statements {#kill-statements}

There are two kinds of kill statements: to kill a query and to kill a mutation

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Attempts to forcibly terminate the currently running queries.
The queries to terminate are selected from the system.processes table using the criteria defined in the `WHERE` clause of the `KILL` query.

Examples:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

Read-only users can only stop their own queries.

By default, the asynchronous version of queries is used (`ASYNC`), which doesn’t wait for confirmation that queries have stopped.

The synchronous version (`SYNC`) waits for all queries to stop and displays information about each process as it stops.
The response contains the `kill_status` column, which can take the following values:

1.  `finished` – The query was terminated successfully.
2.  `waiting` – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can’t be stopped.

A test query (`TEST`) only checks the user’s rights and displays a list of queries to stop.

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tries to cancel and remove [mutations](../../sql-reference/statements/alter/index.md#alter-mutations) that are currently executing. Mutations to cancel are selected from the [`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations) table using the filter specified by the `WHERE` clause of the `KILL` query.

A test query (`TEST`) only checks the user’s rights and displays a list of queries to stop.

Examples:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

Changes already made by the mutation are not rolled back.
