---
description: 'QUOTA に関するドキュメント'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

ユーザーまたはロールに割り当て可能な[クォータ](../../../guides/sre/user-management/index.md#quotas-management)を作成します。

構文:

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

キー `user_name`、`ip_address`、`forwarded_ip_address`、`client_key`、`client_key, user_name`、`client_key, ip_address`、および `normalized_query_hash` は、[system.quotas](../../../operations/system-tables/quotas.md) テーブルのフィールドに対応します。

`IPV4_PREFIX_BITS` および `IPV6_PREFIX_BITS` オプションは、`KEYED BY` が `ip_address` または `forwarded_ip_address` の場合にのみ使用できます。これらは、[system.quotas](../../../operations/system-tables/quotas.md) テーブルのフィールドに対応します。

パラメータ `queries`、`query_selects`、`query_inserts`、`errors`、`result_rows`、`result_bytes`、`read_rows`、`read_bytes`、`written_bytes`、`execution_time`、`failed_sequential_authentications`、`queries_per_normalized_hash` は、[system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md) テーブルのフィールドに対応します。

`ON CLUSTER` 句を使用すると、クラスター上にクォータを作成できます。詳しくは [Distributed DDL](../../../sql-reference/distributed-ddl.md) を参照してください。

**例**

現在のユーザーについて、15 か月間で最大 123 クエリに制限する条件:

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

`default`ユーザーについて、30分あたりの最大実行時間を0.5秒に制限し、さらに5四半期あたりのクエリの最大数を321、エラーの最大数を10に制限します:

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

各正規化クエリパターンごとに専用のバケットを割り当て、実行回数を1時間あたり100回に制限するQUOTAを作成します:

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

各正規化クエリパターンの実行回数を、1時間あたり最大50回に制限します (quota key の種類に関係なく) :

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

XML設定 (ClickHouse Cloud ではサポートされていません) を使用した追加の例については、[Quotas ガイド](/ja/operations/quotas)を参照してください。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseを使用したシングルページアプリケーションの構築](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)