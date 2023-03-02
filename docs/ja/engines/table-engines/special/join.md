---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u53C2\u52A0"
---

# 参加 {#join}

で使用するための準備されたデータ構造 [JOIN](../../../sql-reference/statements/select/join.md#select-join) 作戦だ

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

の詳細な説明を見て下さい [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

**エンジン変数**

-   `join_strictness` – [厳密に結合する](../../../sql-reference/statements/select/join.md#select-join-types).
-   `join_type` – [結合タイプ](../../../sql-reference/statements/select/join.md#select-join-types).
-   `k1[, k2, ...]` – Key columns from the `USING` 句は、 `JOIN` 操作はでなされる。

入力 `join_strictness` と `join_type` 引用符のないパラメータ, `Join(ANY, LEFT, col1)`. も一致しなければならない `JOIN` テーブルが使用される操作。 パラメーターが一致しない場合、ClickHouseは例外をスローせず、誤ったデータを返す可能性があります。

## テーブルの使用法 {#table-usage}

### 例 {#example}

左側のテーブルの作成:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

右側の作成 `Join` テーブル:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

テーブルの結合:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

別の方法として、 `Join` 結合キー値を指定するテーブル:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### データの選択と挿入 {#selecting-and-inserting-data}

以下を使用できます `INSERT` にデータを追加するクエリ `Join`-エンジンテーブル。 テーブルが作成された場合 `ANY` 厳密さ、重複キーのデータは無視されます。 と `ALL` 厳密さは、すべての行が追加されます。

を実行できません。 `SELECT` テーブルから直接クエリ。 代わりに、次のいずれかの方法を使用します:

-   Aの右側にテーブルを置いて下さい `JOIN` 句。
-   コール [joinGet](../../../sql-reference/functions/other-functions.md#joinget) 辞書と同じ方法でテーブルからデータを抽出できる関数。

### 制限事項と設定 {#join-limitations-and-settings}

テーブルを作成するときは、次の設定が適用されます:

-   [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join_any_take_last_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)

その `Join`-エンジンテーブルは使用できません `GLOBAL JOIN` 作戦だ

その `Join`-エンジンは使用を許可する [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) の設定 `CREATE TABLE` 声明。 と [SELECT](../../../sql-reference/statements/select/index.md) クエリを使用できる `join_use_nulls` あまりにも。 あなたが違う場合 `join_use_nulls` 設定することができるエラー入社。 それは結合の種類に依存します。 使用するとき [joinGet](../../../sql-reference/functions/other-functions.md#joinget) 関数、あなたは同じを使用する必要があります `join_use_nulls` 設定 `CRATE TABLE` と `SELECT` 声明。

## データ保存 {#data-storage}

`Join` テーブルデータは常にRAMにあります。 を挿入する際、列表ClickHouseに書き込みデータブロックのディレクトリのディスクできるように復元され、サーバが再起動してしまいます。

場合はサーバが再起動誤り、データブロックのディスクがいます。 この場合、破損したデータを含むファイルを手動で削除する必要がある場合があります。

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/join/) <!--hide-->
