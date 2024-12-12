---
slug: /ja/guides/developer/deduplicating-inserts-on-retries
title: 挿入操作の再試行時のデータ重複防止
description: 挿入操作を再試行する際の重複データ防止策
keywords: [重複排除, デデュプリケート, 挿入再試行, 挿入]
---

挿入操作は、タイムアウトなどのエラーにより失敗することがあります。挿入が失敗した場合、データが成功裏に挿入されたかどうかは不明です。本ガイドでは、挿入再試行時に同じデータが複数回挿入されないように、データ重複防止機能を有効にする方法を説明します。

挿入が再試行されると、ClickHouseはデータが既に成功裏に挿入されたかどうかを判断しようとします。挿入されたデータが重複としてマークされる場合、ClickHouseはそれを対象のテーブルに挿入しません。しかし、ユーザーはあたかもデータが正常に挿入されたかのように成功操作ステータスを受け取ります。

## 再試行時の挿入重複防止の有効化

### テーブルに対する挿入重複防止

**`*MergeTree`エンジンのみが挿入時の重複防止をサポートしています。**

`*ReplicatedMergeTree`エンジンに対しては、挿入重複防止はデフォルトで有効になっており、`replicated_deduplication_window`および`replicated_deduplication_window_seconds`設定によって制御されます。非レプリケートされた`*MergeTree`エンジンに対しては、重複防止は`non_replicated_deduplication_window`設定によって制御されます。

上記の設定は、テーブルの重複防止ログのパラメータを決定します。この重複防止ログは有限の数の`block_id`を保持し、これにより重複防止の仕組みが決まります（以下参照）。

### クエリレベルでの挿入重複防止

設定`insert_deduplicate=1`を用いると、クエリレベルでの重複防止が有効になります。`insert_deduplicate=0`でデータを挿入した場合、そのデータは`insert_deduplicate=1`で挿入を再試行しても重複防止されません。これは、`block_id`が`insert_deduplicate=0`での挿入時に記録されないためです。

## 挿入重複防止の仕組み

データがClickHouseに挿入されるとき、行数とバイト数に基づいてデータがブロックに分割されます。

`*MergeTree`エンジンを使用したテーブルでは、各ブロックにはそのブロック内のデータのハッシュである一意の`block_id`が割り当てられます。この`block_id`は挿入操作の一意キーとして使用されます。同じ`block_id`が重複防止ログで見つかった場合、そのブロックは重複と見なされ、テーブルには挿入されません。

このアプローチは異なるデータを含む挿入の場合にうまく機能します。しかし、同じデータを複数回意図的に挿入する場合は、`insert_deduplication_token`設定を使用して重複防止プロセスを制御する必要があります。この設定を用いると、各挿入に対して一意のトークンを指定でき、ClickHouseはこのトークンを利用してデータが重複かどうかを判断します。

`INSERT ... VALUES` クエリの場合、挿入データのブロック分割は決定的であり、設定によって決定されます。したがって、初回操作と同じ設定値で挿入を再試行することをお勧めします。

`INSERT ... SELECT` クエリの場合、クエリの`SELECT`部分が各操作で同じ順序で同じデータを返すことが重要です。実際の使用においてこれは難しいことに留意してください。再試行で安定したデータ順序を確保するために、クエリの`SELECT`部分で正確な`ORDER BY`セクションを定義してください。再試行間に選択されたテーブルが更新される可能性がある事にも注意が必要です。結果データが変更され、重複防止が行われない可能性があります。また、大量のデータを挿入する場合、挿入後のブロック数が重複防止ログウィンドウをオーバーフローすることがあり、ClickHouseがブロックを重複防止しない可能性があります。

## マテリアライズドビューを使用した挿入重複防止

テーブルにマテリアライズドビューがある場合、そのビューの変換定義にしたがって挿入されたデータもビューの対象に挿入されます。変換されたデータも再試行時に重複を排除されます。ClickHouseはマテリアライズドビューに対しても、ターゲットテーブルに挿入されたデータと同様に重複を排除します。

このプロセスは以下のソーステーブル用の設定を使用して制御できます：
- `replicated_deduplication_window`
- `replicated_deduplication_window_seconds`
- `non_replicated_deduplication_window`

また、ユーザープロファイル設定`deduplicate_blocks_in_dependent_materialized_views`を使用することもできます。

マテリアライズドビュー下のテーブルへブロックを挿入する際、ClickHouseはソーステーブルの`block_id`と追加の識別子を組み合わせた文字列をハッシュして`block_id`を計算します。これにより、マテリアライズドビュー内での正確な重複防止が保証され、元の挿入によってデータが区別され、マテリアライズドビューの対象テーブルに到達する前に加えられる変換にかかわらずデータが識別されます。

## 例

### マテリアライズドビューの変換後の同一ブロック

マテリアライズドビュー内での変換中に生成された同一ブロックは異なる挿入データに基づいているため、重複排除されません。

こちらは例です：

```sql
CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE MATERIALIZED VIEW mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000
AS SELECT
    0 AS key,
    value AS value
FROM dst;
```

```sql
SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;
```

上記の設定により、テーブルから一行のみを持つ一連のブロックを選択できます。これらの小さいブロックは圧縮されず、テーブルに挿入されるまでは同一のままです。

```sql
SET deduplicate_blocks_in_dependent_materialized_views=1;
```

マテリアライズドビューでの重複排除を有効にする必要があります：

```sql
INSERT INTO dst SELECT
    number + 1 AS key,
    IF(key = 0, 'A', 'B') AS value
FROM numbers(2);

SELECT
    *,
    _part
FROM dst
ORDER by all;
```

ここでは、`dst`テーブルに2つのパートが挿入されたことが分かります。選択からの2ブロック — 挿入時の2パート。パートは異なるデータを含んでいます。

```sql
SELECT
    *,
    _part
FROM mv_dst
ORDER by all;
```

ここでは、`mv_dst`テーブルに2つのパートが挿入されたことが分かります。それらのパートは同じデータを含んでいますが、重複排除されていません。

```sql
INSERT INTO dst SELECT
    number + 1 AS key,
    IF(key = 0, 'A', 'B') AS value
FROM numbers(2);

SELECT
    *,
    _part
FROM dst
ORDER by all;

SELECT
    *,
    _part
FROM mv_dst
ORDER by all;
```

ここでは、挿入を再試行したとき、すべてのデータが重複排除されることが確認できます。重複排除は`dst`テーブルと`mv_dst`テーブルの両方で機能します。

## 挿入時の同一ブロック

```sql
CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;


SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;
```

挿入：

```sql
INSERT INTO dst SELECT
    0 AS key,
    'A' AS value
FROM numbers(2);

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;
```

上記の設定で、選択結果から2つのブロックが生成され、その結果、テーブル`dst`への挿入には2つのブロックがあるはずです。しかし、`dst`テーブルには一つのブロックのみが挿入されたことが確認できます。これは、2番目のブロックが重複排除されたためです。挿入されたデータと、重複排除のキーである`block_id`は挿入されたデータのハッシュとして計算されています。この動作は期待されたものではありません。このようなケースは稀ですが、理論的には可能です。このようなケースを正しく処理するために、ユーザーは`insert_deduplication_token`を提供する必要があります。以下の例でそれを修正しましょう：

## `insert_deduplication_token`を使用した挿入時の同一ブロック

```sql
CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;
```

挿入：

```sql
INSERT INTO dst SELECT
    0 AS key,
    'A' AS value
FROM numbers(2)
SETTINGS insert_deduplication_token='some_user_token';

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;
```

2つの同一ブロックが期待通りに挿入されました。

```sql
select 'second attempt';

INSERT INTO dst SELECT
    0 AS key,
    'A' AS value
FROM numbers(2)
SETTINGS insert_deduplication_token='some_user_token';

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;
```

再試行挿入は期待通りに重複排除されます。

```sql
select 'third attempt';

INSERT INTO dst SELECT
    1 AS key,
    'b' AS value
FROM numbers(2)
SETTINGS insert_deduplication_token='some_user_token';

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;
```

この挿入も異なるデータを含んでいるにもかかわらず重複排除されています。`insert_deduplication_token`の優先度が高いことに注意してください：`insert_deduplication_token`が提供されると、ClickHouseはデータのハッシュサムを使用しません。

## 異なる挿入操作がマテリアライズドビューの基になるテーブルで同じデータを生成する場合

```sql
CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE MATERIALIZED VIEW mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000
AS SELECT
    0 AS key,
    value AS value
FROM dst;

SET deduplicate_blocks_in_dependent_materialized_views=1;

select 'first attempt';

INSERT INTO dst VALUES (1, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

SELECT
    'from mv_dst',
    *,
    _part
FROM mv_dst
ORDER by all;

select 'second attempt';

INSERT INTO dst VALUES (2, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

SELECT
    'from mv_dst',
    *,
    _part
FROM mv_dst
ORDER by all;
```

毎回異なるデータを挿入しています。しかし、`mv_dst`テーブルには同じデータが挿入されています。データは元のデータが異なっていたため、重複排除されません。

## 異なるマテリアライズドビューが同一のデータを基になるテーブルに挿入する場合

```sql
CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE TABLE mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE MATERIALIZED VIEW mv_first
TO mv_dst
AS SELECT
    0 AS key,
    value AS value
FROM dst;

CREATE MATERIALIZED VIEW mv_second
TO mv_dst
AS SELECT
    0 AS key,
    value AS value
FROM dst;

SET deduplicate_blocks_in_dependent_materialized_views=1;

select 'first attempt';

INSERT INTO dst VALUES (1, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

SELECT
    'from mv_dst',
    *,
    _part
FROM mv_dst
ORDER by all;
```

テーブル`mv_dst`に2つの等しいブロックが挿入されました（期待通り）。

```sql
select 'second attempt';

INSERT INTO dst VALUES (1, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

SELECT
    'from mv_dst',
    *,
    _part
FROM mv_dst
ORDER by all;
```

この再試行操作は、`dst`テーブルと`mv_dst`テーブルの両方で重複排除されます。
