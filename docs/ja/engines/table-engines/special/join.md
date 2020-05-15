---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 40
toc_title: "\u53C2\u52A0"
---

# 参加 {#join}

Inを使用するための準備済みデータ構造 [JOIN](../../../sql-reference/statements/select.md#select-join) オペレーション

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

**エンジン変数**

-   `join_strictness` – [厳密に結合する](../../../sql-reference/statements/select.md#select-join-strictness).
-   `join_type` – [結合タイプ](../../../sql-reference/statements/select.md#select-join-types).
-   `k1[, k2, ...]` – Key columns from the `USING` その句 `JOIN` 操作はで行われる。

入力 `join_strictness` と `join_type` 引用符なしのパラメーター。, `Join(ANY, LEFT, col1)`. 彼らは `JOIN` テーブルが使用される操作。 パラメータが一致しない場合、ClickHouseは例外をスローせず、誤ったデータを返すことがあります。

## テーブルの使用法 {#table-usage}

### 例えば {#example}

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

代わりとして、データを取り出すことができます `Join` 結合キー値を指定するテーブル:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### データの選択と挿入 {#selecting-and-inserting-data}

を使用することができ `INSERT` データを追加するクエリ `Join`-エンジンテーブル。 テーブルが作成された場合 `ANY` 厳密さ、重複キーのデータは無視されます。 と `ALL` 厳密さは、すべての行が追加されます。

実行することはできません `SELECT` テーブルから直接クエリします。 代わりに、次のいずれかの方法を使用します:

-   テーブルをaの右側に置きます `JOIN` 句。
-   コールを [joinGet](../../../sql-reference/functions/other-functions.md#joinget) この関数を使用すると、テーブルからデータをディクショナリと同じ方法で抽出できます。

### 制限事項と設定 {#join-limitations-and-settings}

テーブルを作成するときは、次の設定が適用されます:

-   [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max\_rows\_in\_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max\_bytes\_in\_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join\_overflow\_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join\_any\_take\_last\_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)

その `Join`-エンジンテーブルは使用できません `GLOBAL JOIN` オペレーション

その `Join`-エンジンは、使用 [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls) の設定 `CREATE TABLE` 声明。 と [SELECT](../../../sql-reference/statements/select.md) クエリは、使用を可能に `join_use_nulls` あまりにも。 あなたが持って異なる `join_use_nulls` 設定は、テーブルを結合エラーを得ることができます。 それは結合の種類に依存します。 使用するとき [joinGet](../../../sql-reference/functions/other-functions.md#joinget) 機能、同じを使用しなければなりません `join_use_nulls` の設定 `CRATE TABLE` と `SELECT` 文。

## データ記憶 {#data-storage}

`Join` テーブルデータは常にRAMにあります。 を挿入する際、列表ClickHouseに書き込みデータブロックのディレクトリのディスクできるように復元され、サーバが再起動してしまいます。

場合はサーバが再起動誤り、データブロックのディスクがいます。 この場合、破損したデータを含むファイルを手動で削除する必要があります。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/join/) <!--hide-->
