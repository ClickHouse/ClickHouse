# Memory overcommit

Memory overcommit is an experimental technique intended to allow to set more flexible memory limits for queries.

The idea of this technique is to introduce settings which can represent guaranteed amount of memory a query can use.
When memory overcommit is enabled and the memory limit is reached ClickHouse will select the most overcommitted query and try to free memory by killing this query.

When memory limit is reached any query will wait some time during attempt to allocate new memory.
If timeout is passed and memory is freed, the query continues execution.
Otherwise an exception will be thrown and the query is killed.

Selection of query to stop or kill is performed by either global or user overcommit trackers depending on what memory limit is reached.
If overcommit tracker can't choose query to stop, MEMORY_LIMIT_EXCEEDED exception is thrown.

## User overcommit tracker

User overcommit tracker finds a query with the biggest overcommit ratio in the user's query list.
Overcommit ratio for a query is computed as number of allocated bytes divided by value of `memory_overcommit_ratio_denominator` setting.

If `memory_overcommit_ratio_denominator` for the query is equals to zero, overcommit tracker won't choose this query.

Waiting timeout is set by `memory_usage_overcommit_max_wait_microseconds` setting.

**Example**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator=4000, memory_usage_overcommit_max_wait_microseconds=500
```

## Global overcommit tracker

Global overcommit tracker finds a query with the biggest overcommit ratio in the list of all queries.
In this case overcommit ratio is computed as number of allocated bytes divided by value of `memory_overcommit_ratio_denominator_for_user` setting.

If `memory_overcommit_ratio_denominator_for_user` for the query is equals to zero, overcommit tracker won't choose this query.

Waiting timeout is set by `global_memory_usage_overcommit_max_wait_microseconds` parameter in the configuration file.
