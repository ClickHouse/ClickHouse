---
description: 'Memory エンジンは、データを非圧縮のまま RAM に格納します。データは、読み込み時に受け取ったそのままの形式で保存されます。言い換えると、このテーブルからの読み取りにはまったくコストがかかりません。'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Memory テーブルエンジン'
doc_type: 'reference'
---

:::note
ClickHouse Cloud で Memory テーブルエンジンを使用する場合、データはすべてのノード間でレプリケートされません (これは仕様です) 。すべてのクエリが同じノードにルーティングされ、Memory テーブルエンジンが想定どおりに動作するようにするには、次のいずれかを行ってください。

* すべての操作を同じセッション内で実行する
* TCP またはネイティブインターフェイス (スティッキー接続をサポート) を使用するクライアントを使う。たとえば [clickhouse-client](/ja/interfaces/client) です
  :::

Memory エンジンは、データを非圧縮のまま RAM に格納します。データは、読み込み時に受け取ったそのままの形式で保存されます。言い換えると、このテーブルからの読み取りにはまったくコストがかかりません。
同時実行のデータアクセスは同期されます。ロック時間は短く、読み取り操作と書き込み操作が互いをブロックすることはありません。
索引はサポートされません。読み取りは並列化されます。

単純なクエリでは、最高のパフォーマンス (10 GB/秒超) が得られます。これは、ディスクからの読み取りや、データの展開、デシリアライズが不要なためです。 (なお、多くの場合、MergeTree エンジンのパフォーマンスもこれに近い水準です。)
サーバーを再起動すると、テーブル内のデータは消え、テーブルは空になります。
通常、このテーブルエンジンを使う妥当な理由はあまりありません。ただし、テスト用途や、比較的少ない行数 (およそ 100,000,000 行まで) に対して最大限の速度が求められるタスクには使用できます。

Memory エンジンは、システム内部で、外部クエリデータ用の一時テーブル (「External data for processing a query」セクションを参照) や、`GLOBAL IN` の実装 (「IN operators」セクションを参照) に使用されます。

上限と下限を指定して Memory エンジンのテーブルサイズを制限できるため、実質的に循環バッファとして動作させることもできます ([エンジンパラメータ](#engine-parameters) を参照) 。

<div id="engine-parameters">
  ## エンジンパラメータ
</div>

* `min_bytes_to_keep` — メモリテーブルにサイズ上限がある場合に保持する最小バイト数。
  * デフォルト値: `0`
  * `max_bytes_to_keep` が必要
* `max_bytes_to_keep` — メモリテーブル内で保持する最大バイト数。挿入のたびに最も古い行が削除されます (つまり循環バッファです) 。大きなブロックを追加する際、削除対象となる最も古い行のバッチを削除すると `min_bytes_to_keep` の制限を下回る場合、最大バイト数は指定した上限を超えることがあります。
  * デフォルト値: `0`
* `min_rows_to_keep` — メモリテーブルにサイズ上限がある場合に保持する最小行数。
  * デフォルト値: `0`
  * `max_rows_to_keep` が必要
* `max_rows_to_keep` — メモリテーブル内で保持する最大行数。挿入のたびに最も古い行が削除されます (つまり循環バッファです) 。大きなブロックを追加する際、削除対象となる最も古い行のバッチを削除すると `min_rows_to_keep` の制限を下回る場合、最大行数は指定した上限を超えることがあります。
  * デフォルト値: `0`
* `compress` - メモリ内のデータを圧縮するかどうか。
  * デフォルト値: `false`

<div id="usage">
  ## 使用方法
</div>

**設定を初期化する**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**設定を変更する**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**注:** `bytes` と `rows` の上限設定用パラメータは同時に設定できますが、`max` と `min` については、より低い制限値が適用されます。

<div id="examples">
  ## 例
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

また、行の場合:

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```