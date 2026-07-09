---
description: 'カラムに関するドキュメント'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: 'カラムの操作'
doc_type: 'reference'
---

テーブル構造を変更するための一連のクエリです。

構文:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

クエリでは、カンマ区切りで 1 つ以上のアクションを指定します。
各アクションはカラムに対する操作です。

次のアクションがサポートされています。

* [ADD COLUMN](#add-column) — テーブルに新しいカラムを追加します。
* [DROP COLUMN](#drop-column) — カラムを削除します。
* [RENAME COLUMN](#rename-column) — 既存のカラムの名前を変更します。
* [CLEAR COLUMN](#clear-column) — カラムの値をリセットします。
* [COMMENT COLUMN](#comment-column) — カラムにコメントを追加します。
* [MODIFY COLUMN](#modify-column) — カラムの型、デフォルト式、有効期限 (TTL)、およびカラム設定を変更します。
* [MODIFY COLUMN REMOVE](#modify-column-remove) — カラムのプロパティのいずれか 1 つを削除します。
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - カラム設定を変更します。
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - カラム設定をリセットします。
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - Enum に新しい値を追加します。
* [MATERIALIZE COLUMN](#materialize-column) — カラムが存在しないパーツで、そのカラムをマテリアライズします。
  これらのアクションについては、以下で詳しく説明します。

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

指定した `name`、`type`、[`codec`](../create/table.md/#column_compression_codec)、および `default_expr` を持つ新しいカラムをテーブルに追加します (`default_expr` については [Default expressions](/ja/sql-reference/statements/create/table#default_values) のセクションを参照してください) 。

`IF NOT EXISTS` 句が含まれている場合、そのカラムがすでに存在していてもクエリはエラーを返しません。`AFTER name_after` (別のカラム名) を指定すると、そのカラムはテーブルのカラム一覧で指定したカラムの直後に追加されます。テーブルの先頭にカラムを追加する場合は、`FIRST` 句を使用します。指定しない場合、カラムはテーブルの末尾に追加されます。一連のアクションでは、`name_after` に、それ以前のいずれかのアクションで追加されたカラム名を指定できます。

カラムを追加しても変更されるのはテーブル構造だけで、データに対しては何も行われません。`ALTER` の実行後も、データはディスク上に書き込まれません。テーブルの読み取り時にそのカラムのデータが存在しない場合は、デフォルト値で補われます (デフォルト式がある場合はそれを実行し、ない場合はゼロまたは空文字列が使用されます) 。このカラムがディスク上に現れるのは、データパーツがマージされた後です ([MergeTree](/ja/engines/table-engines/mergetree-family/mergetree.md) を参照) 。

このアプローチにより、既存データの容量を増やすことなく、`ALTER` クエリを即座に完了できます。

例:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

`name` という名前のカラムを削除します。`IF EXISTS` 句が指定されている場合、そのカラムが存在しなくてもクエリはエラーを返しません。

ファイルシステムからデータを削除します。ファイル全体が削除されるため、クエリはほぼ瞬時に完了します。

:::tip
カラムが [materialized view](/ja/sql-reference/statements/create/view) から参照されている場合、そのカラムは削除できません。削除しようとすると、エラーが返されます。
:::

例:

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

カラム `name` を `new_name` にリネームします。`IF EXISTS` 句が指定されている場合、カラムが存在しなくてもクエリはエラーを返しません。リネームでは基になるデータに変更が加わらないため、クエリはほぼ瞬時に完了します。

**注記**: テーブルのキー式 (`ORDER BY` または `PRIMARY KEY`) で指定されているカラムはリネームできません。これらのカラムを変更しようとすると、`SQL Error [524]` が発生します。

例:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

指定したパーティション内のカラムのすべてのデータをリセットします。パーティション名の指定について詳しくは、[パーティション式の指定方法](../alter/partition.md/#how-to-set-partition-expression)のセクションを参照してください。

`IF EXISTS` 句を指定すると、カラムが存在しない場合でもクエリはエラーを返しません。

例:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

カラムにコメントを追加します。`IF EXISTS` 句が指定されている場合、カラムが存在しなくてもクエリはエラーを返しません。

各カラムには 1 つのコメントを設定できます。カラムにすでにコメントがある場合、新しいコメントで以前のコメントが上書きされます。

コメントは、[DESCRIBE TABLE](/ja/sql-reference/statements/describe-table.md) クエリが返す `comment_expression` カラムに保存されます。

例:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

このクエリは、`name` カラムの以下のプロパティを変更します。

* Type

* デフォルト式

* 圧縮 codec

* 有効期限 (TTL)

* カラムレベル設定

* Enum/Enum8/Enum16 型の Enum 値

カラムの圧縮 CODECS の変更例については、[Column Compression Codecs](../create/table.md/#column_compression_codec)を参照してください。

カラムの TTL の変更例については、[Column TTL](/ja/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl)を参照してください。

カラムレベル設定の変更例については、[Column-level Settings](/ja/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings)を参照してください。

`IF EXISTS` 句が指定されている場合、カラムが存在しなくてもクエリはエラーを返しません。

型を変更する場合、値は [toType](/ja/sql-reference/functions/type-conversion-functions.md) 関数が適用された場合と同様に変換されます。デフォルト式のみを変更する場合、このクエリは複雑な処理を行わず、ほぼ瞬時に完了します。

例:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

カラム型の変更は、唯一複雑な操作です。データを含むファイルの内容が変更されるため、大きなテーブルでは時間がかかることがあります。

このクエリでは、`FIRST | AFTER` 句を使用してカラムの順序を変更することもできます。詳細は [ADD COLUMN](#add-column) の説明を参照してください。ただし、この場合はカラム型の指定が必須です。

例:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

`ALTER` クエリはアトミックです。MergeTree テーブルでは、さらにロックフリーでもあります。

カラムを変更するための `ALTER` クエリはレプリケートされます。命令は ZooKeeper に保存され、その後、各レプリカがそれを適用します。すべての `ALTER` クエリは同じ順序で実行されます。クエリは、他のレプリカで必要な処理が完了するのを待機します。ただし、レプリケートテーブルのカラムを変更するクエリは中断されることがあり、その場合でもすべての処理は非同期に実行されます。

:::note
Nullable カラムを Non-Nullable に変更する際は、十分注意してください。NULL 値が含まれていないことを必ず確認してください。含まれていると、そのカラムの読み取り時に問題が発生します。その場合の回避策は、mutation を kill し、カラムを Nullable 型に戻すことです。
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

カラムのプロパティ (`DEFAULT`、`ALIAS`、`MATERIALIZED`、`CODEC`、`COMMENT`、`TTL`、`SETTINGS` のいずれか 1 つ) を削除します。

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**例**

有効期限 (TTL) を削除:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**関連項目**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

カラム設定を変更します。

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**例**

カラムの `max_compress_block_size` を `1MB` に変更します：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

カラム設定をリセットします。これにより、テーブルの CREATE クエリのカラム式に記述された設定宣言も削除されます。

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**例**

カラム設定 `max_compress_block_size` をデフォルト値に戻します:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

`Enum`、`Enum8`、`Enum16`、`Nullable(Enum)`、`Nullable(Enum8)`、または `Nullable(Enum16)` 型のカラムに、新しい値を追加します

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**例**

カラム `enum_column_name` に2つの値を追加します。

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

`DEFAULT` または `MATERIALIZED` の値式を持つカラムをマテリアライズします。`ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED` でマテリアライズドカラムを追加しても、マテリアライズされた値を持たない既存の行は自動的には補完されません。`MATERIALIZE COLUMN` ステートメントは、`DEFAULT` または `MATERIALIZED` 式を追加または更新したあとに、既存のカラムデータを書き換えるために使用できます (この操作で更新されるのはメタデータのみで、既存のデータ自体は変更されません) 。なお、ソートキーに含まれるカラムをマテリアライズするのは、ソート順序が壊れる可能性があるため無効な操作です。
[mutation](/ja/sql-reference/statements/alter/index.md#mutations) として実装されています。

新規または更新された `MATERIALIZED` の値式を持つカラムでは、既存のすべての行が書き換えられます。

新規または更新された `DEFAULT` の値式を持つカラムでは、動作は ClickHouse のバージョンによって異なります。

* ClickHouse &lt; v24.2 では、既存のすべての行が書き換えられます。
* ClickHouse &gt;= v24.2 では、`DEFAULT` の値式を持つカラムの行の値が、挿入時に明示的に指定されたものか、そうでないか、つまり `DEFAULT` の値式から計算されたものかを区別します。値が明示的に指定されていた場合、ClickHouse はそのまま保持します。値が計算されたものである場合、ClickHouse はそれを新規または更新された `MATERIALIZED` の値式に変更します。

構文:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* PARTITION を指定すると、そのパーティションのみを含むカラムがマテリアライズされます。

**例**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**関連項目**

* [MATERIALIZED](/ja/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## 制限事項
</div>

`ALTER` クエリでは、ネストされたデータ構造内の個別の要素 (カラム) を作成および削除できますが、ネストされたデータ構造全体を作成または削除することはできません。ネストされたデータ構造を追加するには、`name.nested_name` のような名前と `Array(T)` 型を持つカラムを追加します。ネストされたデータ構造は、ドットの前に同じプレフィックスを持つ名前の複数の配列カラムと同等です。

名前にドットを含むカラムのリネームは部分的にサポートされています。ドットは [Nested](/ja/sql-reference/data-types/nested-data-structures/nested) のサブカラムアクセス用に予約されているため、プレフィックス (親名) は同じでなければなりません。変更できるのは接尾辞 (サブカラム名) のみです。たとえば、`a.b` は `a.c` にリネームできますが、`a.b` を `b.d` にリネームすることはできません。これは Nested の親プレフィックスが変わってしまうためです。

主キーまたはサンプリングキー (`ENGINE` 式で使用されるカラム) に含まれるカラムの削除はサポートされていません。主キーに含まれるカラムの型変更は、その変更によってデータの変更が発生しない場合にのみ可能です (たとえば、Enum に値を追加したり、型を `DateTime` から `UInt32` に変更したりすることは許可されています) 。

必要なテーブル変更を行うには `ALTER` クエリだけでは不十分な場合、新しいテーブルを作成し、[INSERT SELECT](/ja/sql-reference/statements/insert-into.md/#inserting-the-results-of-select) クエリを使用してそこへデータをコピーし、その後 [RENAME](/ja/sql-reference/statements/rename.md/#rename-table) クエリでテーブルを切り替え、古いテーブルを削除できます。

`ALTER` クエリは、そのテーブルに対するすべての読み取りと書き込みをブロックします。言い換えると、`ALTER` クエリの実行時に長時間の `SELECT` が実行中であれば、`ALTER` クエリはその完了を待機します。同時に、この `ALTER` の実行中は、同じテーブルに対する新しいクエリもすべて待機します。

自身ではデータを保存しないテーブル ([Merge](/ja/sql-reference/statements/alter/index.md) や [Distributed](/ja/sql-reference/statements/alter/index.md) など) の場合、`ALTER` はテーブル構造を変更するだけで、配下のテーブルの構造は変更しません。たとえば、`Distributed` table に対して `ALTER` を実行する場合は、すべてのリモートサーバー上のテーブルに対しても `ALTER` を実行する必要があります。