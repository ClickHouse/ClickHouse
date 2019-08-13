# SYSTEM Queries {#query_language-system}

- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)


## Managing Distributed Tables {#query_language-system-distributed}

ClickHouse can manage [distributed](../operations/table_engines/distributed.md) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster nodes, then asynchronously sends it. You can manage queue processing with the [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) queries. You can also synchronously insert distributed data with the `insert_distributed_sync` setting.


### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Disables background data distribution when inserting data into distributed tables.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```


### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Forces ClickHouse to send data to cluster nodes synchronously. If any nodes are unavailable, ClickHouse throws an exception and stops query execution. You can retry the query until it succeeds, which will happen when all nodes are back online.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```


### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Enables background data distribution when inserting data into distributed tables.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

[Original article](https://clickhouse.yandex/docs/en/query_language/system/) <!--hide-->
