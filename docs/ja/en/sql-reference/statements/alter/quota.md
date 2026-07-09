---
description: 'QUOTA に関するドキュメント'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

QUOTA を変更します。

構文:

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

キー `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address`, および `normalized_query_hash` は、[system.quotas](../../../operations/system-tables/quotas.md) テーブルのフィールドに対応します。

`IPV4_PREFIX_BITS` および `IPV6_PREFIX_BITS` オプションは、`KEYED BY` が `ip_address` または `forwarded_ip_address` の場合にのみ使用できます。これらは、[system.quotas](../../../operations/system-tables/quotas.md) テーブルのフィールドに対応します。

パラメーター `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `queries_per_normalized_hash` は、[system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) テーブルのフィールドに対応します。

`ON CLUSTER` 句を使用すると、クラスター上にクォータを作成できます。詳細は [Distributed DDL](../../../sql-reference/distributed-ddl.md) を参照してください。

**例**

現在のユーザーについて、15 か月間で最大 123 クエリに制限する制約:

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

`default` ユーザーについて、30 分間で最大実行時間を 0.5 秒に制限し、5 四半期の間にクエリの最大数を 321、エラーの最大数を 10 に制限します:

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```