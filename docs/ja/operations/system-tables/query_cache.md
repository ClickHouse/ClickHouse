---
slug: /ja/operations/system-tables/query_cache
---
# query_cache

[クエリキャッシュ](../query-cache.md)の内容を表示します。

カラム:

- `query` ([String](../../sql-reference/data-types/string.md)) — クエリ文字列。
- `result_size` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリキャッシュエントリのサイズ。
- `tag` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — クエリキャッシュエントリのタグ。
- `stale` ([UInt8](../../sql-reference/data-types/int-uint.md)) — クエリキャッシュエントリが古いかどうか。
- `shared` ([UInt8](../../sql-reference/data-types/int-uint.md)) — クエリキャッシュエントリが複数ユーザー間で共有されているかどうか。
- `compressed` ([UInt8](../../sql-reference/data-types/int-uint.md)) — クエリキャッシュエントリが圧縮されているかどうか。
- `expires_at` ([DateTime](../../sql-reference/data-types/datetime.md)) — クエリキャッシュエントリが古くなる時刻。
- `key_hash` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — クエリ文字列のハッシュで、クエリキャッシュエントリを見つけるためのキーとして使用される。

**例**

``` sql
SELECT * FROM system.query_cache FORMAT Vertical;
```

``` text
Row 1:
──────
query:       SELECT 1 SETTINGS use_query_cache = 1
result_size: 128
tag:
stale:       0
shared:      0
compressed:  1
expires_at:  2023-10-13 13:35:45
key_hash:    12188185624808016954

1 row in set. Elapsed: 0.004 sec.
```
