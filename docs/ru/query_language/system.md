# SYSTEM Queries {#query_language-system}

- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)

## Managing Distributed Tables {#query_language-system-distributed}

ClickHouse can manage [distributed](../operations/table_engines/distributed.md) tables. When a user inserts data into such table, ClickHouse creates a queue of the data which should be sent to servers of the cluster, then asynchronously sends them. You can control the processing of queue by using the requests [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed) and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends).

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Disables asynchronous distribution of data between servers of the cluster.

```
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Forces ClickHouse to send data to the servers of the cluster in synchronous mode. If some of the servers are not available, ClickHouse throws an exception and stops query processing. When servers back into operation, you should repeat the query.

```
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Enables asynchronous distribution of data between servers of cluster.

```
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```
