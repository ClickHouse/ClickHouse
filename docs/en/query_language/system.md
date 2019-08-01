# SYSTEM Queries {#query_language-system}

- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)


## Managing Distributed Tables {#query_language-system-distributed}

ClickHouse can manage [distributed](../operations/table_engines/distributed.md) tables. When a user inserts data into these tables, ClickHouse first creates a queue of the data that should be sent to cluster servers, then asynchronously sends it. You can manage queue processing with the [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed), and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) queries. You can also synchronously insert distributed data with the `insert_distributed_sync` setting.


### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Disables background data distribution when inserting data into distributed tables.

```
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```


### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

Forces ClickHouse to send data to cluster servers synchronously. If any servers are unavailable, ClickHouse throws an exception and stops processing of the query. You should repeat the query when all servers are back online.

```
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```


### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Enables background data distribution when inserting data into distributed tables.

```
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

[Original article](https://clickhouse.yandex/docs/en/query_language/system/) <!--hide-->
