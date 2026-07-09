---
description: 'QUOTA 参考文档'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

创建可分配给用户或角色的[配额](../../../guides/sre/user-management/index.md#quotas-management)。

语法：

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | written_bytes | execution_time | failed_sequential_authentications | queries_per_normalized_hash} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

键 `user_name`、`ip_address`、`forwarded_ip_address`、`client_key`、`client_key, user_name`、`client_key, ip_address` 和 `normalized_query_hash` 对应 [system.quotas](../../../operations/system-tables/quotas.md) 表中的字段。

仅当 `KEYED BY` 为 `ip_address` 或 `forwarded_ip_address` 时，才能使用 `IPV4_PREFIX_BITS` 和 `IPV6_PREFIX_BITS` 选项。它们对应 [system.quotas](../../../operations/system-tables/quotas.md) 表中的字段。

参数 `queries`、`query_selects`、`query_inserts`、`errors`、`result_rows`、`result_bytes`、`read_rows`、`read_bytes`、`written_bytes`、`execution_time`、`failed_sequential_authentications`、`queries_per_normalized_hash` 对应 [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) 表中的字段。

`ON CLUSTER` 子句允许在集群上创建配额，参见 [Distributed DDL](../../../sql-reference/distributed-ddl.md)。

**示例**

使用“15 个月内最多 123 次查询”的约束来限制当前用户的最大查询次数：

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

对于 `default` 用户，将 30 分钟内最大执行时间限制为 0.5 秒，并将 5 个季度内的最大查询次数限制为 321、最大错误次数限制为 10：

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

创建一个 QUOTA，让每个不同的归一化查询模式各自拥有独立的桶，并将其限制为每小时执行 100 次：

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

将任意单个归一化查询模式限制为每小时最多执行 50 次 (与配额键类型无关) ：

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

更多使用 XML 配置 (ClickHouse Cloud 不支持) 的示例，可参见[配额指南](/zh/operations/quotas)。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[使用 ClickHouse 构建单页应用](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)