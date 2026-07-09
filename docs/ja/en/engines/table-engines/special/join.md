---
description: 'JOIN演算で使用するための、任意の事前準備済みデータ構造。'
sidebar_label: 'Join'
sidebar_position: 70
slug: /engines/table-engines/special/join
title: 'Joinテーブルエンジン'
doc_type: 'reference'
---

[JOIN](/ja/sql-reference/statements/select/join)演算で使用するための、任意の事前準備済みデータ構造です。

:::note
ClickHouse Cloud では、サービスが25.4より前のバージョンで作成されている場合、`SET compatibility=25.4` を使用して互換性を25.4以上に設定する必要があります。
:::

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳しい説明を参照してください。

<div id="engine-parameters">
  ## エンジンパラメータ
</div>

<div id="join_strictness">
  ### `join_strictness`
</div>

`join_strictness` – [JOIN の厳密さ](/ja/sql-reference/statements/select/join#supported-types-of-join).

<div id="join_type">
  ### `join_type`
</div>

`join_type` – [JOIN の種類](/ja/sql-reference/statements/select/join#supported-types-of-join).

<div id="key-columns">
  ### キーカラム
</div>

`k1[, k2, ...]` – `JOIN` 操作で使用する `USING` 句のキーカラムです。

`join_strictness` と `join_type` のパラメータは、たとえば `Join(ANY, LEFT, col1)` のように、引用符を付けずに入力します。これらは、そのテーブルを使用する `JOIN` 操作と一致している必要があります。パラメータが一致しない場合、ClickHouse は例外をスローせず、不正確なデータを返す可能性があります。

<div id="specifics-and-recommendations">
  ## 詳細と推奨事項
</div>

<div id="data-storage">
  ### データの保存
</div>

`Join` テーブルのデータは常にRAM上に置かれます。テーブルに行をinsertすると、ClickHouseはサーバーの再起動時に復元できるよう、データブロックをディスク上のディレクトリに書き込みます。

サーバーが正常に再起動されなかった場合、ディスク上のデータブロックが失われたり破損したりすることがあります。この場合、破損したデータを含むファイルを手動で削除する必要があることがあります。

<div id="selecting-and-inserting-data">
  ### データの選択と挿入
</div>

`INSERT` クエリを使用して、Join エンジンのテーブル にデータを追加できます。テーブルが `ANY` strictness で作成されている場合、重複するキーのデータは無視されます。`ALL` strictness の場合は、すべての行が追加されます。

Join エンジンのテーブル の主な用途は次のとおりです。

* `JOIN` 句の右側にテーブルを配置します。
* [joinGet](/ja/sql-reference/functions/other-functions.md/#joinGet) 関数を呼び出します。これにより、Dictionary と同様にテーブルからデータを取得できます。

<div id="deleting-data">
  ### データの削除
</div>

`Join` エンジンのテーブルに対する `ALTER DELETE` クエリは、[ミューテーション](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。`DELETE` ミューテーションは、フィルタに一致するデータを読み取り、メモリおよびディスク上のデータを上書きします。

<div id="join-limitations-and-settings">
  ### 制限事項と設定
</div>

テーブルを作成する際には、次の設定が適用されます。

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

[join&#95;use&#95;nulls](/ja/operations/settings/settings.md/#join_use_nulls)

<div id="max_rows_in_join">
  #### `max_rows_in_join`
</div>

[max&#95;rows&#95;in&#95;join](/ja/operations/settings/settings#max_rows_in_join)

<div id="max_bytes_in_join">
  #### `max_bytes_in_join`
</div>

[max&#95;bytes&#95;in&#95;join](/ja/operations/settings/settings#max_bytes_in_join)

<div id="join_overflow_mode">
  #### `join_overflow_mode`
</div>

[join&#95;overflow&#95;mode](/ja/operations/settings/settings#join_overflow_mode)

<div id="join_any_take_last_row">
  #### `join_any_take_last_row`
</div>

[join&#95;any&#95;take&#95;last&#95;row](/ja/operations/settings/settings.md/#join_any_take_last_row)

<div id="join_use_nulls">
  #### `join_use_nulls`
</div>

<div id="persistent">
  #### 永続化
</div>

Join および [Set](/ja/engines/table-engines/special/set.md) テーブルエンジンの永続化を無効にします。

I/O のオーバーヘッドを削減します。パフォーマンスを重視し、永続化が不要な用途に適しています。

設定可能な値:

* 1 — 有効。
* 0 — 無効。

デフォルト値: `1`。

Join エンジンのテーブル は `GLOBAL JOIN` 操作では使用できません。

`Join`-engine では、`CREATE TABLE` ステートメントで [join&#95;use&#95;nulls](/ja/operations/settings/settings.md/#join_use_nulls) 設定を指定できます。[SELECT](/ja/sql-reference/statements/select/index.md) クエリでも、`join_use_nulls` の値を同じにする必要があります。

<div id="example">
  ## 使用例
</div>

左側のテーブルを作成します：

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

右側の`Join`テーブルの作成:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

テーブルの結合:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

別の方法として、join key の値を指定して `Join` テーブルからデータを取得できます。

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

`Join` テーブルから行を削除するには:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```