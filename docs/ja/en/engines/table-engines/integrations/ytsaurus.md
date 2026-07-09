---
description: 'YTsaurusクラスターからデータをインポートできるテーブルエンジン。'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'YTsaurusテーブルエンジン'
keywords: ['YTsaurus', 'テーブルエンジン']
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # YTsaurus テーブルエンジン
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

YTsaurus テーブルエンジンを使用すると、YTsaurusクラスターからデータをインポートできます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
これは実験的な機能であり、将来のリリースで後方互換性のない形で変更される可能性があります。
設定 [`allow_experimental_ytsaurus_table_engine`](/ja/operations/settings/settings#allow_experimental_ytsaurus_table_engine) を使用して、
YTsaurus テーブルエンジン の使用を有効にします。

次のように有効にできます。

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**エンジンパラメータ**

* `http_proxy_url` — YTsaurus HTTP プロキシの URL。
* `cypress_path` — データソースへの Cypress パス。
* `oauth_token` — OAuth トークン。

<div id="usage-example">
  ## 使用例
</div>

YTsaurus テーブルを作成するクエリは次のとおりです。

```sql title="Query"
SHOW CREATE TABLE yt_saurus;
```

```sql title="Response"
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')
```

テーブルからデータを取得するには、次を実行します：

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## データ型
</div>

<div id="primitive-data-types">
  ### プリミティブデータ型
</div>

| YTsaurus データ型               | ClickHouse データ型 |
| --------------------------- | --------------- |
| `int8`                      | `Int8`          |
| `int16`                     | `Int16`         |
| `int32`                     | `Int32`         |
| `int64`                     | `Int64`         |
| `uint8`                     | `UInt8`         |
| `uint16`                    | `UInt16`        |
| `uint32`                    | `UInt32`        |
| `uint64`                    | `UInt64`        |
| `float`                     | `Float32`       |
| `double`                    | `Float64`       |
| `boolean`                   | `Bool`          |
| `string`                    | `String`        |
| `utf8`                      | `String`        |
| `json`                      | `JSON`          |
| `yson(type_v3)`             | `JSON`          |
| `uuid`                      | `UUID`          |
| `date32`                    | `Date`(未対応)     |
| `datetime64`                | `Int64`         |
| `timestamp64`               | `Int64`         |
| `interval64`                | `Int64`         |
| `date`                      | `Date`(未対応)     |
| `datetime`                  | `DateTime`      |
| `timestamp`                 | `DateTime64(6)` |
| `interval`                  | `UInt64`        |
| `any`                       | `String`        |
| `null`                      | `Nothing`       |
| `void`                      | `Nothing`       |
| `T` with `required = False` | `Nullable(T)`   |

<div id="composite-data-types">
  ### 複合データ型
</div>

| YTsaurus データ型 | ClickHouse データ型        |
| ------------- | ---------------------- |
| `decimal`     | `Decimal`              |
| `optional`    | `Nullable`             |
| `list`        | `Array`                |
| `struct`      | `NamedTuple`           |
| `tuple`       | `Tuple`                |
| `variant`     | `Variant`              |
| `dict`        | &#96;Array(Tuple(...)) |
| `tagged`      | `T`                    |

**関連項目**

* [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md) テーブル関数
* [ytsaurus データスキーマ](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [ytsaurus データ型](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)