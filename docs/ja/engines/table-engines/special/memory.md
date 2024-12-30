---
slug: /ja/engines/table-engines/special/memory
sidebar_position: 110
sidebar_label: Memory
---

# Memory テーブルエンジン

:::note
ClickHouse CloudでMemoryテーブルエンジンを使用する際、データは全ノード間でレプリケーションされません（設計によるもの）。全クエリを同一ノードにルーティングし、Memoryテーブルエンジンが期待通りに動作することを保証するために、以下のいずれかを行うことができます：
- 同じセッション内で全ての操作を実行する
- [clickhouse-client](/ja/interfaces/cli)のようなTCPまたはネイティブインターフェイスを使用するクライアントを使用する（これによりスティッキー接続のサポートが有効になる）
:::

MemoryエンジンはデータをRAMに非圧縮形式で保存します。データは読み込まれる時に受け取った形式と全く同じ形で保存されます。言い換えれば、このテーブルからの読み取りは完全に無料です。並行データアクセスは同期されています。ロックは短く、読み取りと書き込み操作が互いをブロックすることはありません。インデックスはサポートされていません。読み取りは並列化されています。

シンプルなクエリで最大の生産性（10 GB/秒以上）が得られます。なぜなら、ディスクからの読み取りやデータの解凍、逆シリアル化がないためです。（多くの場合、MergeTreeエンジンの生産性はほぼ同等であることを記しておくべきです。）サーバーを再起動すると、データはテーブルから消え、テーブルは空になります。通常、このテーブルエンジンを使用する理由はありません。しかし、テストや比較的小さい行数（おおよそ1億行まで）で最大の速度が必要なタスクには使用することができます。

Memoryエンジンは、外部クエリデータを含む一時テーブルや、`GLOBAL IN`の実装（「INオペレーター」のセクションを参照）にシステムによって使用されます。

Memoryエンジンテーブルサイズを制限するために上下限を指定することができ、これにより循環バッファとして機能させることが可能です（[エンジンパラメーター](#engine-parameters)参照）。

## エンジンパラメーター {#engine-parameters}

- `min_bytes_to_keep` — サイズ制限がある場合にメモリテーブルに保持する最小バイト数。
  - デフォルト値: `0`
  - `max_bytes_to_keep`が必要
- `max_bytes_to_keep` — メモリテーブル内で保持する最大バイト数。この値を超えると各挿入で最古の行が削除されます（つまり循環バッファ）。大きなブロックを追加する際に、削除する最古の行のバッチが`min_bytes_to_keep`の制限下にあると、この最大バイト数の制限を超えることがあります。
  - デフォルト値: `0`
- `min_rows_to_keep` — サイズ制限がある場合にメモリテーブルに保持する最小行数。
  - デフォルト値: `0`
  - `max_rows_to_keep`が必要
- `max_rows_to_keep` — メモリテーブル内で保持する最大行数。この値を超えると各挿入で最古の行が削除されます（つまり循環バッファ）。大きなブロックを追加する際に、削除する最古の行のバッチが`min_rows_to_keep`の制限下にあると、この最大行数の制限を超えることがあります。
  - デフォルト値: `0`

## 使用法 {#usage}

**設定の初期化**
``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**設定の変更**
```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**注:** `bytes`と`rows`の両方の制限パラメーターは同時に設定可能ですが、`max`および`min`の下限が遵守されます。

## 例 {#examples}
``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. 最古のブロックが保持されている（3000行） */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. 削除されないブロックを追加 */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. 古いブロックが削除される（9216 bytes - 1100） */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. 全てを上書きする非常に大きなブロックをチェック */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();
```

``` text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

また、行数についても：

``` sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. 最古のブロックが保持されている（3000行） */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. 削除されないブロックを追加 */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. 古いブロックが削除される（9216 bytes - 1100） */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. 全てを上書きする非常に大きなブロックをチェック */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' and database = currentDatabase();
```

``` text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```
