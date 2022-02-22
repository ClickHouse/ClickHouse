# Memory overcommit

Memory overcommit is an experimental technique intended to allow to set more flexible memory limits for queries.

The idea of this technique is to introduce settings which can represent guaranteed memory amount of memory a query can use.
When memory overcommit is enabled and memory limit is reached ClickHouse will select the most overcommit query and try to free memory by killing this query.

When memory limit is reached any query will wait some time during atempt to allocate new memory.
If selected query is killed and memory is freed within waiting timeout, query will continue execution after waiting, otherwise it'll be killed too.

Selection of query to stop is performed by either global or user overcommit trackers depending on what memory limit is reached.

## User overcommit tracker

User overcommit tracker finds a query with the biggest overcommit ratio in the user's query list.
Overcommit ratio is computed as number of allocated bytes divided by value of `max_guaranteed_memory_usage` setting.

Waiting timeout is set by `memory_usage_overcommit_max_wait_microseconds` setting.

**Example**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS max_guaranteed_memory_usage=4000, memory_usage_overcommit_max_wait_microseconds=500
```

## Global overcommit tracker

Global overcommit tracker finds a query with the biggest overcommit ratio in the list of all queries.
In this case overcommit ratio is computed as number of allocated bytes divided by value of `max_guaranteed_memory_usage_for_user` setting.

Waiting timeout is set by `global_memory_usage_overcommit_max_wait_microseconds` parameter in the configuration file.
