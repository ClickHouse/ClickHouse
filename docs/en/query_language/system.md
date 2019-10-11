# SYSTEM Queries {#query_language-system}

- [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
- [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
- [DROP DNS CACHE](#query_language-system-drop-dns-cache)
- [DROP MARKS CACHE](#query_language-system-drop-marks-cache)
- [FLUSH LOGS](#query_language-system-flush_logs)
- [RELOAD CONFIG](#query_language-system-reload-config)
- [SHUTDOWN](#query_language-system-shutdown)
- [KILL](#query_language-system-kill)
- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Reloads all dictionaries that have been successfully loaded before.
By default, dictionaries are loaded lazily (see [dictionaries_lazy_load](../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load)), so instead of being loaded automatically at startup, they are initialized on first access through dictGet function or SELECT from tables with ENGINE = Dictionary. The `SYSTEM RELOAD DICTIONARIES` query reloads such dictionaries (LOADED).
Always returns `Ok.` regardless of the result of the dictionary update.

## RELOAD DICTIONARY dictionary_name {#query_language-system-reload-dictionary}

Completely reloads a dictionary `dictionary_name`, regardless of the state of the dictionary (LOADED / NOT_LOADED / FAILED).
Always returns `Ok.` regardless of the result of updating the dictionary.
The status of the dictionary can be checked by querying the `system.dictionaries` table.

```sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Resets ClickHouse's internal DNS cache. Sometimes (for old ClickHouse versions) it is necessary to use this command when changing the infrastructure (changing the IP address of another ClickHouse server or the server used by dictionaries).

For more convenient (automatic) cache management, see disable_internal_dns_cache, dns_cache_update_period parameters.

## DROP MARKS CACHE {#query_language-system-drop-marks-cache}

Resets the mark cache. Used in development of ClickHouse and performance tests.

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

Reloads ClickHouse configuration. Used when configuration is stored in ZooKeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Normally shuts down ClickHouse (like `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Aborts ClickHouse process (like `kill -9 {$ pid_clickhouse-server}`)

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
