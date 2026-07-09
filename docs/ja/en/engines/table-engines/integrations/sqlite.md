---
description: 'このエンジンでは、SQLite へのデータのインポートとエクスポートが可能で、ClickHouse から SQLite のテーブルに対して直接クエリを実行できます。'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'SQLite table engine'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # SQLite table engine
</div>

<CloudNotSupportedBadge />

このエンジンでは、SQLite との間でデータをインポートおよびエクスポートでき、ClickHouse から SQLite テーブルに対して直接クエリを実行することもできます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**エンジンパラメータ**

* `db_path` — データベースが格納された SQLite ファイルへのパス。
* `table` — SQLite データベース内のテーブル名、またはそのまま SQLite に渡されるクエリ ([テーブル名の代わりにクエリを指定する](#passing-a-query)を参照) 。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを渡す
</div>

テーブル名の代わりに、`table` 引数には、そのまま SQLite に渡される `SELECT` クエリを指定できます。テーブルの構造はクエリ結果から推論されます。クエリは、サブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

このようなテーブルは読み取り専用で、これに対する `INSERT` は許可されていません。同じ構文は [`sqlite`](/ja/sql-reference/table-functions/sqlite) テーブル関数でもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は ClickHouse によって解析され、SQLite に送信される前に再シリアライズされます。したがって、有効な ClickHouse SQL である必要があります。ClickHouse が解析しない SQLite 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式のテキストは、そのまま SQLite に送信されます。

また、渡されたクエリには、周囲の ClickHouse クエリにある外側の `WHERE`、`LIMIT`、aggregation などは **プッシュダウンされず**、完全なクエリ結果を取得した後に ClickHouse 側で適用されます。SQLite から読み取るデータを制限するには、フィルターを渡すクエリの中に記述してください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を指定すると、プッシュダウンできない外側のフィルターはローカルで適用される代わりに、例外として拒否されます。
:::

<div id="data-types-support">
  ## データ型のサポート
</div>

テーブル定義で ClickHouse のカラム型を明示的に指定すると、SQLite の TEXT カラムから次の ClickHouse 型をパースできます。

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* すべての整数型 ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

デフォルトの型マッピングについては、[SQLite データベースエンジン](../../../engines/database-engines/sqlite.md#data_types-support) を参照してください。

<div id="usage-example">
  ## 使用例
</div>

SQLiteテーブルを作成するクエリの例を示します。

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

テーブルのデータを返します:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**関連項目**

* [SQLite](../../../engines/database-engines/sqlite.md) エンジン
* [sqlite](../../../sql-reference/table-functions/sqlite.md) テーブル関数