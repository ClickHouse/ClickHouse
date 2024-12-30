---
slug: /ja/sql-reference/statements/alter/quota
sidebar_position: 46
sidebar_label: QUOTA
title: "ALTER QUOTA"
---

クオータを変更します。

構文:

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | client_key | client_key,user_name | client_key,ip_address} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```
`user_name`、`ip_address`、`client_key`、`client_key, user_name`、`client_key, ip_address`は、[system.quotas](../../../operations/system-tables/quotas.md)テーブルのフィールドに対応しています。

`queries`、`query_selects`、'query_inserts'、`errors`、`result_rows`、`result_bytes`、`read_rows`、`read_bytes`、`execution_time`は、[system.quotas_usage](../../../operations/system-tables/quotas_usage.md)テーブルのフィールドに対応しています。

`ON CLUSTER`句はクラスター上でクオータを作成することを可能にします。詳細は[分散DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

**例**

現在のユーザーに対して15ヶ月で123のクエリの最大数を制限する例:

``` sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

デフォルトユーザーに対して、30分で0.5秒の最大実行時間を制限し、5四半期でクエリの最大数を321、エラーの最大数を10に制限する例:

``` sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

