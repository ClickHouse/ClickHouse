---
description: 'QUOTA 文档'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

修改配额。

语法：

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

键 `user_name`、`ip_address`、`forwarded_ip_address`、`client_key`、`client_key, user_name`、`client_key, ip_address` 和 `normalized_query_hash` 对应 [system.quotas](../../../operations/system-tables/quotas.md) 表中的字段。

`IPV4_PREFIX_BITS` 和 `IPV6_PREFIX_BITS` 选项只能在 `KEYED BY` 为 `ip_address` 或 `forwarded_ip_address` 时使用。它们对应 [system.quotas](../../../operations/system-tables/quotas.md) 表中的字段。

参数 `queries`、`query_selects`、`query_inserts`、`errors`、`result_rows`、`result_bytes`、`read_rows`、`read_bytes`、`execution_time`、`queries_per_normalized_hash` 对应 [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) 表中的字段。

`ON CLUSTER` 子句允许在集群上创建配额，参见 [Distributed DDL](../../../sql-reference/distributed-ddl.md)。

**示例**

将当前用户在 15 个月内的最大查询数限制为 123：

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

对于 `default` 用户：在 30 分钟内将最大执行时间限制为 0.5 秒，将最大查询数限制为 321，并在 5 个 15 分钟周期内将最大错误数限制为 10：

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```