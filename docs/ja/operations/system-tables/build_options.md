---
slug: /ja/operations/system-tables/build_options
---
# build_options

ClickHouseサーバーのビルドオプションに関する情報が含まれています。

カラム:

- `name` (String) — ビルドオプションの名前、例えば `USE_ODBC`
- `value` (String) — ビルドオプションの値、例えば `1`

**例**

``` sql
SELECT * FROM system.build_options LIMIT 5
```

``` text
┌─name─────────────┬─value─┐
│ USE_BROTLI       │ 1     │
│ USE_BZIP2        │ 1     │
│ USE_CAPNP        │ 1     │
│ USE_CASSANDRA    │ 1     │
│ USE_DATASKETCHES │ 1     │
└──────────────────┴───────┘
```
