---
sidebar_position: 42
sidebar_label: QUOTA
---

# CREATE QUOTA

Creates a [quota](../../../operations/access-rights.md#quotas-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {user_name | ip_address | client_key | client_key,user_name | client_key,ip_address} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Keys `user_name`, `ip_address`, `client_key`, `client_key, user_name` and `client_key, ip_address` correspond to the fields in the [system.quotas](../../../operations/system-tables/quotas.md) table.

Parameters `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time` correspond to the fields in the [system.quotas_usage](../../../operations/system-tables/quotas_usage.md) table.

`ON CLUSTER` clause allows creating quotas on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

**Examples**

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

For the default user limit the maximum execution time with half a second in 30 minutes, and limit the maximum number of queries with 321 and the maximum number of errors with 10 in 5 quaters:

``` sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```
