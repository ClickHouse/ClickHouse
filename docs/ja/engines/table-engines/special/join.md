---
slug: /ja/engines/table-engines/special/join
sidebar_position: 70
sidebar_label: Join
---

# Join テーブルエンジン

[JOIN](/docs/ja/sql-reference/statements/select/join.md/#select-join) 操作で使用するためのオプションの準備済みデータ構造です。

:::note
これは [JOIN句](/docs/ja/sql-reference/statements/select/join.md/#select-join) そのものに関する記事ではありません。
:::

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

[CREATE TABLE](/docs/ja/sql-reference/statements/create/table.md/#create-table-query) クエリの詳細な説明を参照してください。

## エンジンパラメータ

### join_strictness

`join_strictness` – [JOIN厳密性](/docs/ja/sql-reference/statements/select/join.md/#select-join-types)。

### join_type

`join_type` – [JOIN型](/docs/ja/sql-reference/statements/select/join.md/#select-join-types)。

### キーカラム

`k1[, k2, ...]` – `JOIN` 操作で `USING` 句から使われるキーカラムです。

`join_strictness` と `join_type` パラメータは、例として `Join(ANY, LEFT, col1)` のように引用符なしで入力します。これらはテーブルが使用される `JOIN` 操作に一致する必要があります。パラメータが一致しない場合、ClickHouse は例外をスローせず、不正確なデータを返す可能性があります。

## 特長と推奨事項 {#specifics-and-recommendations}

### データストレージ {#data-storage}

`Join` テーブルのデータは常に RAM に置かれます。テーブルに行を挿入する際、ClickHouse はデータブロックをディスク上のディレクトリに書き込み、サーバーが再起動した際に復元できるようにします。

サーバーが不正に再起動すると、ディスク上のデータブロックが失われたり破損したりする可能性があります。この場合、破損したデータのファイルを手動で削除する必要があるかもしれません。

### データの選択と挿入 {#selecting-and-inserting-data}

`Join` エンジンのテーブルにデータを追加するために `INSERT` クエリを使用できます。テーブルが `ANY` 厳密性で作成された場合、重複したキーのデータは無視されます。`ALL` 厳密性の場合、すべての行が追加されます。

`Join` エンジンのテーブルの主な使用ケースは以下の通りです:

- `JOIN` 句でテーブルを右側に配置する。
- 同じ方法でテーブルからデータを抽出できる [joinGet](/docs/ja/sql-reference/functions/other-functions.md/#joinget) 関数を呼び出す。

### データの削除 {#deleting-data}

`Join` エンジンのテーブルに対する `ALTER DELETE` クエリは、[変異](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。`DELETE` 変異はフィルタリングされたデータを読み取り、メモリとディスクのデータを上書きします。

### 制限と設定 {#join-limitations-and-settings}

テーブルを作成する際、以下の設定が適用されます:

#### join_use_nulls

[join_use_nulls](/docs/ja/operations/settings/settings.md/#join_use_nulls)

#### max_rows_in_join

[max_rows_in_join](/docs/ja/operations/settings/query-complexity.md/#settings-max_rows_in_join)

#### max_bytes_in_join

[max_bytes_in_join](/docs/ja/operations/settings/query-complexity.md/#settings-max_bytes_in_join)

#### join_overflow_mode

[join_overflow_mode](/docs/ja/operations/settings/query-complexity.md/#settings-join_overflow_mode)

#### join_any_take_last_row

[join_any_take_last_row](/docs/ja/operations/settings/settings.md/#join_any_take_last_row)

### persistent

Join と [Set](/docs/ja/engines/table-engines/special/set.md) テーブルエンジンに対する永続性を無効にします。

I/O のオーバーヘッドを削減します。パフォーマンスを追求し永続性が必要ないシナリオに適しています。

可能な値:

- 1 — 有効。
- 0 — 無効。

デフォルト値: `1`。

`Join` エンジンのテーブルは `GLOBAL JOIN` 操作で使用することはできません。

`Join` エンジンでは [join_use_nulls](/docs/ja/operations/settings/settings.md/#join_use_nulls) 設定を `CREATE TABLE` ステートメントで指定できます。[SELECT](/docs/ja/sql-reference/statements/select/index.md) クエリは同じ `join_use_nulls` 値を持っている必要があります。

## 使用例 {#example}

左側のテーブルを作成:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13);
```

右側の `Join` テーブルを作成:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23);
```

テーブルを結合:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

代替として、結合キーの値を指定して `Join` テーブルからデータを取得することができます:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

`Join` テーブルから行を削除:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```
