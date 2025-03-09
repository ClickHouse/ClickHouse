---
slug: /ja/sql-reference/statements/alter/column
sidebar_position: 37
sidebar_label: COLUMN
title: "カラム操作"
---

テーブル構造を変更するための一連のクエリ。

構文:

``` sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

クエリでは、カンマで区切ったアクションのリストを指定します。 各アクションはカラムへの操作です。

サポートされているアクションは以下の通りです:

- [ADD COLUMN](#add-column) — 新しいカラムをテーブルに追加します。
- [DROP COLUMN](#drop-column) — カラムを削除します。
- [RENAME COLUMN](#rename-column) — 既存のカラムの名前を変更します。
- [CLEAR COLUMN](#clear-column) — カラムの値をリセットします。
- [COMMENT COLUMN](#comment-column) — カラムにテキストコメントを追加します。
- [MODIFY COLUMN](#modify-column) — カラムのタイプ、デフォルト表現、有効期限 (TTL)、カラム設定を変更します。
- [MODIFY COLUMN REMOVE](#modify-column-remove) — カラムのプロパティのうちの1つを削除します。
- [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - カラム設定を変更します。
- [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - カラム設定をリセットします。
- [MATERIALIZE COLUMN](#materialize-column) — カラムが欠落している部分でカラムを物理化します。
これらのアクションについては、以下で詳しく説明します。

## ADD COLUMN

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

指定された`name`、`type`、[`codec`](../create/table.md/#column_compression_codec)、および`default_expr`で新しいカラムをテーブルに追加します（[デフォルト表現](/docs/ja/sql-reference/statements/create/table.md/#create-default-values)を参照）。

`IF NOT EXISTS`句が含まれている場合、カラムがすでに存在していてもクエリはエラーを返しません。`AFTER name_after`（他のカラムの名前）を指定した場合、そのカラムはテーブルカラムのリストにおける指定されたカラムの後に追加されます。テーブルの先頭にカラムを追加したい場合は、`FIRST`句を使用します。それ以外の場合、カラムはテーブルの末尾に追加されます。アクションのチェーンでは、`name_after`は前のアクションで追加されたカラムの名前になることがあります。

カラムを追加することは、データに対して何らかのアクションを行うことなくテーブル構造を変更するだけです。`ALTER`の後、データはディスクに現れません。テーブルから読み込む際にカラムのデータが欠落している場合、デフォルト値（もしあればデフォルト表現の実行によって、またはゼロや空文字列を使用することによって）が埋め込まれます。このカラムは、データ部分をマージした後にディスクに現れます（[MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)を参照）。

このアプローチにより、`ALTER`クエリを即座に完了させ、古いデータの量を増やさないようにしています。

例:

``` sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

``` text
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

## DROP COLUMN

``` sql
DROP COLUMN [IF EXISTS] name
```

`name`という名前のカラムを削除します。`IF EXISTS`句を指定した場合、カラムが存在しなくてもクエリはエラーを返しません。

ファイルシステムからデータを削除します。これはファイルを丸ごと削除するため、クエリはほぼ即座に完了します。

:::tip
[Materialized View](/docs/ja/sql-reference/statements/create/view.md/#materialized)で参照されているカラムは削除できません。それ以外の場合はエラーを返します。
:::

例:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

## RENAME COLUMN

``` sql
RENAME COLUMN [IF EXISTS] name to new_name
```

カラム`name`を`new_name`に名前変更します。`IF EXISTS`句を指定した場合、カラムが存在しなくてもクエリはエラーを返しません。名前変更は基となるデータに影響しないため、クエリはほぼ即座に完了します。

**注意**: テーブルのキー表現に指定されたカラム（`ORDER BY`または`PRIMARY KEY`で指定されたカラム）は名前変更できません。これらのカラムを変更しようとすると、`SQL Error [524]`が発生します。

例:

``` sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

## CLEAR COLUMN

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

指定されたパーティションのカラム内のすべてのデータをリセットします。[パーティション表現の設定方法](../alter/partition.md/#how-to-set-partition-expression)のセクションでパーティション名の設定について詳しく説明しています。

`IF EXISTS`句を指定した場合、カラムが存在しなくてもクエリはエラーを返しません。

例:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

## COMMENT COLUMN

``` sql
COMMENT COLUMN [IF EXISTS] name 'テキストコメント'
```

カラムにコメントを追加します。`IF EXISTS`句を指定した場合、カラムが存在しなくてもクエリはエラーを返しません。

各カラムには1つのコメントを持つことができます。カラムに既にコメントが存在する場合、新しいコメントが以前のコメントを上書きします。

コメントは、[DESCRIBE TABLE](/docs/ja/sql-reference/statements/describe-table.md)クエリによって返される`comment_expression`カラムに保存されます。

例:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

## MODIFY COLUMN

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
ALTER COLUMN [IF EXISTS] name TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
```

このクエリは、`name`カラムのプロパティを変更します:

- タイプ

- デフォルト表現

- 圧縮コーデック

- 有効期限 (TTL)

- カラムレベルの設定

カラム圧縮コーデックの変更例については、[カラム圧縮コーデック](../create/table.md/#column_compression_codec)を参照してください。

カラムTTLの変更例については、[カラムTTL](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl)を参照してください。

カラムレベル設定の変更例については、[カラムレベル設定](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings)を参照してください。

`IF EXISTS`句を指定した場合、カラムが存在しなくてもクエリはエラーを返しません。

タイプを変更する際には、[toType](/docs/ja/sql-reference/functions/type-conversion-functions.md)関数が適用されたかのように値が変換されます。デフォルト表現のみを変更する場合、クエリは複雑な操作を行わず、ほぼ即座に完了します。

例:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

カラムタイプの変更は唯一の複雑なアクションであり、データを含むファイルの内容を変更します。大規模なテーブルの場合、この操作には長時間かかることがあります。

クエリは`FIRST | AFTER`句を使用してカラムの順序を変更することもでき、詳細は[ADD COLUMN](#add-column)の説明を参照してください。ただし、この場合、カラムタイプは必須です。

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

`ALTER`クエリは原子的です。MergeTreeテーブルに対してもロックフリーです。

カラムを変更するための`ALTER`クエリはレプリケートされます。指示はZooKeeperに保存され、その後各レプリカがそれを適用します。すべての`ALTER`クエリは同じ順序で実行されます。クエリは、他のレプリカ上で適切なアクションが完了するのを待ちます。しかし、レプリケートされたテーブルでカラムを変更するためのクエリは中断可能で、すべての操作は非同期で実行されます。

## MODIFY COLUMN REMOVE

カラムのプロパティのうちの1つを削除します: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS`.

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**例**

TTLの削除:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**関連項目**

- [REMOVE TTL](ttl.md).

## MODIFY COLUMN MODIFY SETTING

カラム設定を変更します。

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**例**

カラムの`max_compress_block_size`を`1MB`に変更:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

## MODIFY COLUMN RESET SETTING

カラム設定をリセットし、テーブルのCREATEクエリのカラム式に宣言されている設定を削除します。

構文:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**例**

カラム設定`max_compress_block_size`をデフォルト値にリセット:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

## MATERIALIZE COLUMN

`DEFAULT`または`MATERIALIZED`値表現を持つカラムを物理化します。`ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`を使用して物理化されたカラムを追加する際、既存の行に物理化された値が自動的に埋め込まれません。`MATERIALIZE COLUMN`文は、`DEFAULT`または`MATERIALIZED`表現が追加または更新された後に既存のカラムデータを書き換えるために使用されます（既存のデータを変更することなくメタデータのみを更新します）。
これは[mutation](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

新しく追加または更新された`MATERIALIZED`値表現を持つカラムの場合、すべての既存行が書き換えられます。

新しく追加または更新された`DEFAULT`値表現を持つカラムの場合、挙動はClickHouseのバージョンに依存します:
- ClickHouse < v24.2では、すべての既存行が書き換えられます。
- ClickHouse >= v24.2では、クリックハウスが挿入時に行の値が明示的に指定されたか、または`DEFAULT`値表現から計算されたかを区別します。値が明示的に指定された場合、ClickHouseはそのまま保持します。計算された場合、新しく追加または更新された`MATERIALIZED`値表現に変更します。

構文:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```
- パーティションを指定した場合、そのパーティションのみが物理化されます。

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

- [MATERIALIZED](/docs/ja/sql-reference/statements/create/table.md/#materialized).

## 制限事項

`ALTER`クエリは、ネストされたデータ構造内の個々の要素（カラム）を作成および削除することを可能にしますが、全体のネストされたデータ構造を操作することはできません。ネストされたデータ構造を追加するには、`name.nested_name`のような名前と`Array(T)`タイプでカラムを追加できます。ネストされたデータ構造は、ドットの前に同じプレフィックスを持つ複数の配列カラムと等価です。

主キーやサンプリングキー（`ENGINE`式で使用されるカラム）のカラムを削除するサポートはありません。主キーに含まれるカラムのタイプを変更するのは、この変更がデータを変更しない場合に限り可能です（例えば、Enumの値を追加したり、`DateTime`から`UInt32`に型を変更することが可能です）。

`ALTER`クエリで必要なテーブル変更が十分でない場合、新しいテーブルを作成し、[INSERT SELECT](/docs/ja/sql-reference/statements/insert-into.md/#inserting-the-results-of-select)クエリを使用してデータをコピーし、[RENAME](/docs/ja/sql-reference/statements/rename.md/#rename-table)クエリを使用してテーブルを切り替え、古いテーブルを削除することができます。

`ALTER`クエリはテーブルのすべての読み取りおよび書き込みをブロックします。つまり、`ALTER`クエリの時に長時間の`SELECT`が実行されている場合、`ALTER`クエリは完了を待ちます。同時に、同じテーブルへのすべての新しいクエリは`ALTER`が実行されている間、待機します。

データを自体で保存していないテーブル（例えば[Merge](/docs/ja/sql-reference/statements/alter/index.md) や [分散テーブル](/docs/ja/sql-reference/statements/alter/index.md)など）に対する`ALTER`は単にテーブル構造を変更するだけであり、従属するテーブルの構造を変更しません。例えば、`分散テーブル`に対して`ALTER`を実行する場合、リモートサーバー上のテーブルに対しても`ALTER`を実行する必要があります。
