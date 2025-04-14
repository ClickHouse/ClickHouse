---
slug: /ja/sql-reference/statements/alter/partition
sidebar_position: 38
sidebar_label: PARTITION
title: "パーティションとパーツの操作"
---

[パーティション](/docs/ja/engines/table-engines/mergetree-family/custom-partitioning-key.md)に対する以下の操作が可能です：

- [DETACH PARTITION\|PART](#detach-partitionpart) — パーティションまたはパーツを`detached`ディレクトリに移動し、忘れます。
- [DROP PARTITION\|PART](#drop-partitionpart) — パーティションまたはパーツを削除します。
- [DROP DETACHED PARTITION\|PART](#drop-detached-partitionpart) - `detached`から指定されたパートまたはすべてのパーティションのパーツを削除します。
- [FORGET PARTITION](#forget-partition) — パーティションが空の場合、ZooKeeperからそのメタデータを削除します。
- [ATTACH PARTITION\|PART](#attach-partitionpart) — `detached`ディレクトリからテーブルにパーティションまたはパーツを追加します。
- [ATTACH PARTITION FROM](#attach-partition-from) — データパーティションを他のテーブルからコピーして追加します。
- [REPLACE PARTITION](#replace-partition) — データパーティションを他のテーブルからコピーし、置き換えます。
- [MOVE PARTITION TO TABLE](#move-partition-to-table) — データパーティションを他のテーブルに移動します。
- [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — パーティション内の指定されたカラムの値をリセットします。
- [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — パーティション内の指定されたセカンダリインデックスをリセットします。
- [FREEZE PARTITION](#freeze-partition) — パーティションのバックアップを作成します。
- [UNFREEZE PARTITION](#unfreeze-partition) — パーティションのバックアップを削除します。
- [FETCH PARTITION\|PART](#fetch-partitionpart) — 他のサーバーからパーツまたはパーティションをダウンロードします。
- [MOVE PARTITION\|PART](#move-partitionpart) — パーティションまたはデータパーツを他のディスクまたはボリュームに移動します。
- [UPDATE IN PARTITION](#update-in-partition) — パーティション内のデータを条件に基づいて更新します。
- [DELETE IN PARTITION](#delete-in-partition) — パーティション内のデータを条件に基づいて削除します。

## DETACH PARTITION\|PART

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

指定されたパーティションのすべてのデータを`detached`ディレクトリに移動します。サーバーは、このデタッチされたデータパーティションを存在しないものとして忘れます。このデータについてサーバーは、[ATTACH](#attach-partitionpart)クエリを行うまで認識しません。

例：

``` sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

パーティション式の設定については、[パーティション式の設定方法](#how-to-set-partition-expression)セクションを参照してください。

クエリが実行された後、`detached`ディレクトリのデータに対して好きな操作を行うことができます。ファイルシステムから削除するか、そのままにしておくことが可能です。

このクエリはレプリケートされ、すべてのレプリカでデータを`detached`ディレクトリに移動します。このクエリを実行できるのは、リーダーレプリカ上のみです。レプリカがリーダーかどうかを確認するには、[system.replicas](/docs/ja/operations/system-tables/replicas.md/#system_tables-replicas)テーブルへの`SELECT`クエリを実行します。あるいは、すべてのレプリカで`DETACH`クエリを実行するのが簡単で、リーダーレプリカ以外のすべてのレプリカは例外を投げます（複数のリーダーを許可するとは言え、リーダーレプリカのみがクエリを処理します）。

## DROP PARTITION\|PART

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

指定されたパーティションをテーブルから削除します。このクエリはパーティションを非アクティブとしてタグ付けし、約10分でデータを完全に削除します。

パーティション式の設定については、[パーティション式の設定方法](#how-to-set-partition-expression)セクションを参照してください。

このクエリはレプリケートされ、すべてのレプリカからデータを削除します。

例：

``` sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

## DROP DETACHED PARTITION\|PART

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

`detached`から指定されたパーツまたは指定されたパーティションのすべてのパーツを削除します。
パーティション式の設定については、[パーティション式の設定方法](#how-to-set-partition-expression)セクションを参照してください。

## FORGET PARTITION

``` sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

ZooKeeperから空のパーティションに関するすべてのメタデータを削除します。パーティションが空でないか、不明な場合はクエリが失敗します。再利用されないパーティションに対してのみ実行してください。

パーティション式の設定については、[パーティション式の設定方法](#how-to-set-partition-expression)セクションを参照してください。

例：

``` sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

## ATTACH PARTITION\|PART

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] ATTACH PARTITION|PART partition_expr
```

`detached`ディレクトリからテーブルにデータを追加します。全体のパーティションまたは個別のパーツについてデータを追加することができます。例：

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

パーティション式の設定については、[パーティション式の設定方法](#how-to-set-partition-expression)セクションを参照してください。

このクエリはレプリケートされます。レプリカ・イニシエーターは`detached`ディレクトリにデータがあるか確認し、データが存在する場合、その整合性をチェックします。すべてが正しければ、データをテーブルに追加します。

非イニシエーターのレプリカが添付コマンドを受け取った際に、正しいチェックサムを持つパーツが自身の`detached`フォルダに見つかれば、他のレプリカからデータを取得せずにデータを添付します。
パーツが正しいチェックサムを持つ場合は、データは該当パーツを持つ任意のレプリカからダウンロードされます。

データを1つのレプリカの`detached`ディレクトリに配置し、`ALTER ... ATTACH`クエリを使用してデータをすべてのレプリカのテーブルに追加することができます。

## ATTACH PARTITION FROM

``` sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

このクエリは、`table1`から`table2`にデータパーティションをコピーします。

注意点：

- `table1`や`table2`からデータは削除されません。
- `table1`は一時テーブルである可能性があります。

クエリを成功させるためには、次の条件を満たす必要があります：

- 両方のテーブルが同じ構造でなければならない。
- 両方のテーブルが同じパーティションキー、ソートキー、主キーを持っている必要がある。
- 両方のテーブルが同じインデックスとプロジェクションを持っている必要がある。
- 両方のテーブルが同じストレージポリシーを持っている必要がある。

## REPLACE PARTITION

``` sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

このクエリは、`table1`から`table2`にデータパーティションをコピーし、`table2`の既存のパーティションを置き換えます。操作はアトミックです。

注意点：

- `table1`からデータは削除されません。
- `table1`は一時テーブルである可能性があります。

クエリを成功させるためには、次の条件を満たす必要があります：

- 両方のテーブルが同じ構造でなければならない。
- 両方のテーブルが同じパーティションキー、ソートキー、主キーを持っている必要がある。
- 両方のテーブルが同じインデックスとプロジェクションを持っている必要がある。
- 両方のテーブルが同じストレージポリシーを持っている必要がある。

## MOVE PARTITION TO TABLE

``` sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

このクエリは、`table_source`から`table_dest`にデータパーティションを移動し、`table_source`からデータを削除します。

クエリを成功させるためには、次の条件を満たす必要があります：

- 両方のテーブルが同じ構造でなければならない。
- 両方のテーブルが同じパーティションキー、ソートキー、主キーを持っている必要がある。
- 両方のテーブルが同じインデックスとプロジェクションを持っている必要がある。
- 両方のテーブルが同じストレージポリシーを持っている必要がある。
- 両方のテーブルが同じエンジンファミリーでなければならない（レプリケートまたは非レプリケート）。

## CLEAR COLUMN IN PARTITION

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

指定されたパーティション内の特定のカラムのすべての値をリセットします。テーブル作成時に`DEFAULT`句が指定されていた場合、このクエリはカラムの値を指定されたデフォルト値に設定します。

例：

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

## FREEZE PARTITION

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

このクエリは、指定されたパーティションのローカルバックアップを作成します。`PARTITION`句が省略された場合、すべてのパーティションのバックアップが一度に作成されます。

:::note
バックアッププロセス全体がサーバーを停止せずに行われます。
:::

名前が指定されたパーティションのバックアップを削除します。`PARTITION`句が省略された場合、すべてのパーティションのバックアップが一度に削除されます。

:::note
データストレージ用のディスクセットをテーブルに使用する場合、`PARTITION`式に一致するデータパーツを格納する`shadow/N`ディレクトリが各ディスクに作成されます。
:::

バックアップ作成時間には、データスナップショット用に、テーブルデータにハードリンクが作成されます。ハードリンクは、`/var/lib/clickhouse/shadow/N/...`ディレクトリに配置され、ここで：

- `/var/lib/clickhouse/`は設定で指定されたClickHouseの作業ディレクトリです。
- `N`はバックアップのインクリメンタル番号です。
- `WITH NAME`パラメータが指定されている場合、インクリメンタル番号の代わりに`'backup_name'`パラメータの値が使用されます。

このバックアップはほぼ瞬時に作成されます（ただし、まず対応するテーブルに現在のクエリが終了するのを待機します）。

`ALTER TABLE t FREEZE PARTITION`はデータのみをコピーし、テーブルのメタデータはコピーしません。テーブルメタデータのバックアップを取るには、`/var/lib/clickhouse/metadata/database/table.sql`ファイルをコピーします。

バックアップからデータを復元するには、次の操作を行います：

1. テーブルが存在しない場合は作成します。クエリを見るには、`.sql`ファイルを使用します（`ATTACH`を`CREATE`に置き換えます）。
2. バックアップ内の`data/database/table/`ディレクトリから`/var/lib/clickhouse/data/database/table/detached/`ディレクトリにデータをコピーします。
3. `ALTER TABLE t ATTACH PARTITION`クエリを実行して、データをテーブルに追加します。

バックアップからの復元はサーバーの停止を必要としません。

バックアップおよびデータ復元についての詳細は、[データバックアップ](/docs/ja/operations/backup.md)セクションを参照してください。

## UNFREEZE PARTITION

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

指定された名前を持つ`freezed`パーティションをディスクから削除します。`PARTITION`句が省略された場合、すべてのパーティションのバックアップが一度に削除されます。

## CLEAR INDEX IN PARTITION

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

このクエリは、`CLEAR COLUMN`と同様に機能しますが、カラムデータの代わりにインデックスをリセットします。

## FETCH PARTITION|PART

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

他のサーバーからパーティションをダウンロードします。このクエリはレプリケートされたテーブルでのみ動作します。

このクエリは次の動作を行います：

1. 指定されたシャードからパーティション|パートをダウンロードします。'path-in-zookeeper'には、ZooKeeper内でのシャードへのパスを指定する必要があります。
2. 次に、クエリはダウンロードしたデータを`table_name`テーブルの`detached`ディレクトリに配置します。[ATTACH PARTITION\|PART](#attach-partitionpart)クエリを使用してデータをテーブルに追加します。

例：

1. FETCH PARTITION
``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```
2. FETCH PART
``` sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

注意点：

- `ALTER ... FETCH PARTITION|PART`クエリはレプリケートされません。それはデータまたはパーティションをローカルサーバーの`detached`ディレクトリにのみ配置します。
- `ALTER TABLE ... ATTACH`クエリはレプリケートされます。それはデータをすべてのレプリカに追加します。データは`detached`ディレクトリから1つのレプリカに追加され、他のレプリカには隣接するレプリカから追加されます。

ダウンロードする前に、システムはパーティションが存在し、テーブル構造が一致するかをチェックします。最も適切なレプリカは、健全なレプリカから自動的に選択されます。

このクエリは`ALTER TABLE`と呼ばれていますが、テーブル構造を変更したり、即座にテーブル内の利用可能なデータを変更することはありません。

## MOVE PARTITION\|PART

`MergeTree`エンジンテーブルのパーティションまたはデータパーツを別のボリュームまたはディスクに移動します。詳細は[データストレージ用に複数のブロックデバイスを使用する](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes)を参照してください。

``` sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

`ALTER TABLE t MOVE`クエリ：

- レプリケートされません。異なるレプリカは異なるストレージポリシーを持つことができるためです。
- 指定されたディスクまたはボリュームが構成されていない場合、エラーを返します。また、ストレージポリシーで指定された移動条件が適用できない場合にもエラーを返します。
- データが既にバックグラウンドプロセスによって移動している、同時の`ALTER TABLE t MOVE`クエリまたはバックグラウンドデータマージの結果として移動している場合には、エラーを返すことがあります。この場合、ユーザーは追加の操作を行う必要はありません。

例：

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

## UPDATE IN PARTITION

指定されたフィルタリング式にマッチするパーティション内のデータを操作します。これは[mutation](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

構文：

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

### 例

``` sql
-- パーティション名を使用
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- パーティションIDを使用
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

### 関連項目

- [UPDATE](/docs/ja/sql-reference/statements/alter/update.md/#alter-table-update-statements)

## DELETE IN PARTITION

指定されたフィルタリング式にマッチするパーティション内のデータを削除します。これは[mutation](/docs/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

構文：

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

### 例

``` sql
-- パーティション名を使用
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- パーティションIDを使用
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

### 関連項目

- [DELETE](/docs/ja/sql-reference/statements/alter/delete.md/#alter-mutations)

## パーティション式の設定方法

`ALTER ... PARTITION`クエリでパーティション式を設定するには、さまざまな方法があります：

- `system.parts`テーブルの`partition`カラムの値として指定します。例：`ALTER TABLE visits DETACH PARTITION 201901`。
- キーワード`ALL`を使用します。これはDROP/DETACH/ATTACH/ATTACH FROMでのみ使用できます。例：`ALTER TABLE visits ATTACH PARTITION ALL`。
- テーブルのパーティションキーのタプルと型が一致する式や定数のタプルとして指定します。単一要素のパーティションキーの場合、式を`tuple(...)`関数で包む必要があります。例：`ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`。
- パーティションIDを使用します。パーティションIDはファイルシステムやZooKeeperでパーティションの名前として使われる（可能であれば人間が読める文字列の）識別子です。パーティションIDは`PARTITION ID`句で引用符内に指定しなければなりません。例：`ALTER TABLE visits DETACH PARTITION ID '201901'`。
- [ALTER ATTACH PART](#attach-partitionpart)および[DROP DETACHED PART](#drop-detached-partitionpart)クエリでは、`system.detached_parts`テーブルの`name`カラムからの値を使用して、パーツの名前を文字列リテラルで指定します。例：`ALTER TABLE visits ATTACH PART '201901_1_1_0'`。

パーティションを指定する際の引用符の使用は、パーティション式のタイプに依存します。例えば、`String`タイプの場合、その名前を引用符（`'`）で囲む必要があります。`Date`や`Int*`タイプの場合、引用符は不要です。

上記の規則は、[OPTIMIZE](/docs/ja/sql-reference/statements/optimize.md)クエリにも適用されます。パーティション化されていないテーブルを最適化する際に特定のパーティションのみを指定する必要がある場合、式を`PARTITION tuple()`と設定してください。例：

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION`は、`ALTER TABLE`クエリの結果として`UPDATE`または`DELETE`式が適用されるパーティションを指定します。指定されたパーティションからのみ新しいパーツが作成されます。これにより、パーティションが多く分かれている場合でも、インデックスを更新する必要があるポイントだけにデータを更新する負荷を減らすことができます。

`ALTER ... PARTITION`クエリの例は、テスト[`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql)および[`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql)で示されています。
