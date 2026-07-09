---
description: 'PARTITION に関するドキュメント'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'パーティションとパーツの操作'
doc_type: 'reference'
---

[パーティション](/ja/engines/table-engines/mergetree-family/custom-partitioning-key.md)では、次の操作を実行できます：

* [DETACH PARTITION|PART](#detach-partitionpart) — パーティションまたはパーツを `detached` ディレクトリに移動し、管理対象から外します。
* [DROP PARTITION|PART](#drop-partitionpart) — パーティションまたはパーツを削除します。
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - `detached` から、パーツ、またはパーティションに含まれるすべてのパーツを削除します。
* [FORGET PARTITION](#forget-partition) — パーティションが空の場合、ZooKeeper からそのメタデータを削除します。
* [ATTACH PARTITION|PART](#attach-partitionpart) — `detached` ディレクトリからパーティションまたはパーツをテーブルに追加します。
* [ATTACH PARTITION FROM](#attach-partition-from) — あるテーブルから別のテーブルへデータパーティションをコピーして追加します。
* [REPLACE PARTITION](#replace-partition) — あるテーブルから別のテーブルへデータパーティションをコピーして置き換えます。
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — あるテーブルから別のテーブルへデータパーティションを移動します。
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — パーティション内の指定したカラムの値をリセットします。
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — パーティション内の指定したセカンダリ索引をリセットします。
* [FREEZE PARTITION](#freeze-partition) — パーティションのバックアップを作成します。
* [UNFREEZE PARTITION](#unfreeze-partition) — パーティションのバックアップを削除します。
* [FETCH PARTITION|PART](#fetch-partitionpart) — 別のサーバーからパーツまたはパーティションをダウンロードします。
* [MOVE PARTITION|PART](#move-partitionpart) — パーティションまたはデータパーツを別のディスクまたはボリュームに移動します。
* [UPDATE IN PARTITION](#update-in-partition) — 条件に基づいてパーティション内のデータを更新します。
* [DELETE IN PARTITION](#delete-in-partition) — 条件に基づいてパーティション内のデータを削除します。
* [REWRITE PARTS](#rewrite-parts) — テーブル内のパーツ (または特定のパーティション内のパーツ) を完全に書き換えます。

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

指定したパーティションのすべてのデータを `detached` ディレクトリに移動します。サーバーは、デタッチされたデータパーティションを存在しないものとして扱い、認識しなくなります。[ATTACH](#attach-partitionpart) クエリを実行するまで、サーバーはこのデータを認識しません。

例:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

パーティション式の指定については、[パーティション式の指定方法](#how-to-set-partition-expression) のセクションを参照してください。

クエリの実行後は、`detached` ディレクトリ内のデータを自由に扱えます。ファイルシステムから削除しても、そのまま残しておいても構いません。

このクエリはレプリケートされます。つまり、すべてのレプリカでデータが `detached` ディレクトリに移動されます。このクエリを実行できるのはリーダーレプリカ上だけである点に注意してください。レプリカがリーダーかどうかを確認するには、[system.replicas](/ja/operations/system-tables/replicas) テーブルに対して `SELECT` クエリを実行します。あるいは、すべてのレプリカで `DETACH` クエリを実行したほうが簡単です。リーダーレプリカ以外のすべてのレプリカは例外をスローします (複数のリーダーが許可されているため) 。

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

指定したパーティションをテーブルから削除します。このクエリはパーティションを非アクティブとしてマークし、約10分後にデータを完全に削除します。

パーティション式の指定については、[パーティション式の指定方法](#how-to-set-partition-expression)の節を参照してください。

このクエリはレプリケートされるため、すべてのレプリカ上のデータが削除されます。

例:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

指定したパーツ、または指定したパーティション内のすべてのパーツを`detached`から削除します。
パーティション式の指定について詳しくは、[パーティション式の指定方法](#how-to-set-partition-expression)を参照してください。

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

空のパーティションに関するすべてのメタデータをZooKeeperから削除します。パーティションが空でない場合、または存在しない場合、このクエリは失敗します。今後二度と使用しないパーティションに対してのみ実行してください。

パーティション式の指定については、[パーティション式の指定方法](#how-to-set-partition-expression) のセクションを参照してください。

例:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

`detached` ディレクトリからテーブルにデータを追加します。パーティション全体のデータ、または個別のパーツのデータを追加できます。例:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

パーティション式の指定方法について詳しくは、[パーティション式の指定方法](#how-to-set-partition-expression) の節を参照してください。

このクエリはレプリケートされます。イニシエーターであるレプリカは、`detached` ディレクトリにデータがあるかどうかを確認します。
データが存在する場合、クエリはその整合性を確認します。問題がなければ、クエリはそのデータをテーブルに追加します。

attach コマンドを受け取ったイニシエーター以外のレプリカは、自身の `detached` ディレクトリ内に正しいチェックサムを持つパーツを見つけた場合、他のレプリカから取得せずにそのデータをアタッチします。
正しいチェックサムを持つパーツが存在しない場合、そのデータはそのパーツを持ついずれかのレプリカからダウンロードされます。

あるレプリカの `detached` ディレクトリにデータを配置し、`ALTER ... ATTACH` クエリを使用して、すべてのレプリカ上のテーブルにそのデータを追加できます。

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

このクエリは、データのパーティションを `table1` から `table2` にコピーします。

注意:

* `table1` と `table2` のどちらのデータも削除されません。
* `table1` には一時テーブルを使用できます。

クエリを正常に実行するには、次の条件を満たす必要があります。

* 両方のテーブルの構造が同一である必要があります。
* 両方のテーブルで、パーティションキー、ORDER BY キー、主キーが同一である必要があります。
* 両方のテーブルで、同じストレージポリシーを使用している必要があります。
* 宛先テーブルには、ソーステーブルのすべての索引とプロジェクションが含まれている必要があります。宛先テーブルで `enforce_index_structure_match_on_partition_manipulation` 設定が有効になっている場合、索引とプロジェクションは完全に一致している必要があります。そうでない場合、宛先テーブルにはソーステーブルの索引およびプロジェクションをすべて含んだ上位集合を持たせることができます。

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

このクエリは、データのパーティションを `table1` から `table2` にコピーし、`table2` の既存のパーティションを置き換えます。この操作はアトミックです。

注意:

* `table1` からデータが削除されることはありません。
* `table1` は一時テーブルでもかまいません。

クエリを正常に実行するには、次の条件を満たす必要があります。

* 両方のテーブルが同じ構造である必要があります。
* 両方のテーブルが、同じパーティションキー、同じ ORDER BY キー、および同じ主キーを持っている必要があります。
* 両方のテーブルが同じストレージポリシーを持っている必要があります。
* 宛先テーブルには、ソーステーブルのすべての索引とプロジェクションが含まれている必要があります。宛先テーブルで `enforce_index_structure_match_on_partition_manipulation` 設定が有効になっている場合、索引とプロジェクションは完全に一致している必要があります。そうでない場合、宛先テーブルはソーステーブルの索引とプロジェクションの上位集合を持つことができます。

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

このクエリは、`table_source` のデータを削除しながら、データのパーティションを `table_source` から `table_dest` へ移動します。

クエリを正常に実行するには、次の条件を満たしている必要があります。

* 両方のテーブルの構造が同一であること。
* 両方のテーブルが同じパーティションキー、同じ ORDER BY キー、および同じ主キーを持つこと。
* 両方のテーブルが同じストレージポリシーを持つこと。
* 両方のテーブルが同じエンジンファミリー (replicated または non-replicated) であること。
* 宛先テーブルには、ソーステーブルのすべての索引とプロジェクションが含まれている必要があります。宛先テーブルで `enforce_index_structure_match_on_partition_manipulation` 設定が有効な場合、索引とプロジェクションは完全に一致している必要があります。そうでない場合、宛先テーブルはソーステーブルの索引とプロジェクションをすべて含む上位集合であってもかまいません。

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

指定したパーティションの指定したカラムにあるすべての値をリセットします。テーブル作成時に `DEFAULT` 句が指定されていた場合、このクエリはそのカラムの値を指定されたデフォルト値に設定します。

例:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

このクエリは、指定したパーティションのローカルバックアップを作成します。`PARTITION` 句を省略した場合は、すべてのパーティションのバックアップを一度に作成します。

:::note
バックアップ処理全体は、サーバーを停止せずに実行されます。
:::

古い形式のテーブルでは、パーティション名のプレフィックス (たとえば `2019`) を指定できます。その場合、このクエリは対応するすべてのパーティションのバックアップを作成します。パーティション式の設定については、[パーティション式の指定方法](#how-to-set-partition-expression) セクションを参照してください。

実行時、このクエリはデータのスナップショット用にテーブルデータへのハードリンクを作成します。ハードリンクは `/var/lib/clickhouse/shadow/N/...` ディレクトリに配置されます。ここで:

* `/var/lib/clickhouse/` は、config で指定された ClickHouse の作業ディレクトリです。
* `N` はバックアップの連番です。
* `WITH NAME` パラメータが指定されている場合は、連番の代わりに `'backup_name'` パラメータの値が使用されます。

:::note
[テーブルのデータストレージに複数のディスクを使用している](/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes)場合、`shadow/N` ディレクトリは各ディスク上に作成され、`PARTITION` 式に一致するデータパーツが保存されます。
:::

バックアップ内には、`/var/lib/clickhouse/` と同じディレクトリ構造が作成されます。このクエリはすべてのファイルに対して `chmod` を実行し、書き込みを禁止します。

バックアップの作成後は、`/var/lib/clickhouse/shadow/` からリモートサーバーへデータをコピーし、その後ローカルサーバーから削除できます。なお、`ALTER t FREEZE PARTITION` クエリはレプリケートされません。ローカルサーバー上にのみローカルバックアップを作成します。

このクエリはほぼ瞬時にバックアップを作成します (ただし最初に、対象テーブルに対して現在実行中のクエリの完了を待ちます) 。

`ALTER TABLE t FREEZE PARTITION` はデータのみをコピーし、テーブルのメタデータはコピーしません。テーブルのメタデータもバックアップするには、`/var/lib/clickhouse/metadata/database/table.sql` ファイルをコピーしてください。

バックアップからデータを復元するには、次の手順を実行します。

1. テーブルが存在しない場合は作成します。クエリを確認するには `.sql` ファイルを使用し、その中の `ATTACH` を `CREATE` に置き換えます。
2. バックアップ内の `data/database/table/` ディレクトリから、`/var/lib/clickhouse/data/database/table/detached/` ディレクトリにデータをコピーします。
3. `ALTER TABLE t ATTACH PARTITION` クエリを実行して、データをテーブルに追加します。

バックアップからの復元でも、サーバーを停止する必要はありません。

このクエリはパーツを並列に処理し、スレッド数は `max_threads` 設定によって制御されます。

バックアップとデータの復元の詳細については、[&quot;ClickHouse でのバックアップと復元&quot;](/ja/operations/backup/overview) セクションを参照してください。

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

指定した名前の `frozen` パーティションをディスクから削除します。`PARTITION` 句を省略した場合、クエリによってすべてのパーティションのバックアップが一括で削除されます。

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

このクエリは `CLEAR COLUMN` と似た動作をしますが、カラムデータではなく索引をリセットします。

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

別のサーバーからパーティションをダウンロードします。このクエリはレプリケートテーブルでのみ動作します。

このクエリは次の処理を行います。

1. 指定した分片からパーティション|パーツをダウンロードします。&#39;path-in-zookeeper&#39; には、ZooKeeper 内の分片へのパスを指定する必要があります。
2. 次に、このクエリはダウンロードしたデータを `table_name` テーブルの `detached` ディレクトリに配置します。データをテーブルに追加するには、[ATTACH PARTITION|PART](#attach-partitionpart) クエリを使用します。

例:

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

次の点に注意してください。

* `ALTER ... FETCH PARTITION|PART` クエリはレプリケートされません。パーツ またはパーティションは、ローカルサーバー上の `detached` ディレクトリにのみ配置されます。
* `ALTER TABLE ... ATTACH` クエリはレプリケートされます。これにより、すべてのレプリカにデータが追加されます。データは、ある 1 つのレプリカには `detached` ディレクトリから追加され、その他のレプリカには近隣のレプリカから追加されます。

ダウンロードの前に、システムはパーティションが存在することと、テーブル構造が一致していることを確認します。正常なレプリカの中から、最も適切なレプリカが自動的に選択されます。

このクエリは `ALTER TABLE` と呼ばれていますが、テーブル構造を変更するものではなく、テーブルで利用可能なデータも直ちには変更しません。

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

`MergeTree` エンジンのテーブルで、パーティションまたはデータパーツを別のボリュームまたはディスクに移動します。詳細は [データストレージに複数のブロックデバイスを使用する](/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes) を参照してください。

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

`ALTER TABLE t MOVE` クエリの特徴は次のとおりです。

* レプリケートされません。レプリカごとに異なるストレージポリシーを使用できるためです。
* 指定したディスクまたはボリュームが設定されていない場合は、error を返します。また、ストレージポリシーで指定されたデータ移動の条件を適用できない場合も、クエリは error を返します。
* 移動対象のデータが、バックグラウンドプロセス、同時実行の `ALTER TABLE t MOVE` クエリ、またはバックグラウンドでのデータマージによってすでに移動されている場合は、error を返すことがあります。この場合、ユーザーが追加の操作を行う必要はありません。

例:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

指定したフィルタリング式に一致する、指定したパーティション内のデータを操作します。[mutation](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

構文:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### 例
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### 関連項目
</div>

* [UPDATE](/ja/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

指定したパーティション内で、指定したフィルタリング式に一致するデータを削除します。[mutation](/ja/sql-reference/statements/alter/index.md#mutations) として実装されています。

構文:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### 例
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

これにより、すべての新しい設定を使って、パーツが最初から再書き込みされます。これは、`use_const_adaptive_granularity` のようなテーブルレベルの設定が、デフォルトでは新しく書き込まれるパーツにのみ適用されるためです。

<div id="example">
  ### 例
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### 関連項目
</div>

* [DELETE](/ja/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## パーティション式の指定方法
</div>

`ALTER ... PARTITION` クエリでは、パーティション式をさまざまな方法で指定できます。

* `system.parts` テーブルの `partition` カラムの値として指定します。たとえば、`ALTER TABLE visits DETACH PARTITION 201901` です。
* キーワード `ALL` を使用します。これは DROP/DETACH/ATTACH/ATTACH FROM でのみ使用できます。たとえば、`ALTER TABLE visits ATTACH PARTITION ALL` です。
* テーブルのパーティション化キーのタプルと型が一致する、式または定数のタプルとして指定します。パーティション化キーが単一要素の場合、式は `tuple (...)` 関数で囲む必要があります。たとえば、`ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))` です。
* partition ID を使用します。partition ID はパーティションの文字列識別子で、可能であれば人間が読みやすい形式となっており、ファイルシステムおよび ZooKeeper ではパーティション名として使用されます。partition ID は `PARTITION ID` 句で、シングルクォート付きで指定する必要があります。たとえば、`ALTER TABLE visits DETACH PARTITION ID '201901'` です。
* [ALTER ATTACH PART](#attach-partitionpart) および [DROP DETACHED PART](#drop-detached-partitionpart) クエリでパーツ名を指定するには、[system.detached&#95;parts](/ja/operations/system-tables/detached_parts) テーブルの `name` カラムの値を文字列リテラルとして使用します。たとえば、`ALTER TABLE visits ATTACH PART '201901_1_1_0'` です。

パーティション指定時のクォートの使い方は、パーティション式の型によって異なります。たとえば、`String` 型では、その名前をクォート (`'`) 付きで指定する必要があります。`Date` 型および `Int*` 型ではクォートは不要です。

上記のルールはすべて [OPTIMIZE](/ja/sql-reference/statements/optimize.md) クエリにも当てはまります。パーティション化されていないテーブルを最適化する際に唯一のパーティションを指定する必要がある場合は、式 `PARTITION tuple()` を設定します。たとえば、次のとおりです。

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` は、`ALTER TABLE` クエリによって [UPDATE](/ja/sql-reference/statements/alter/update) または [DELETE](/ja/sql-reference/statements/alter/delete) の式が適用されるパーティションを指定します。新しいパーツは、指定したパーティションからのみ作成されます。これにより、テーブルが多数のパーティションに分割されていて、データを個別に更新するだけでよい場合に、`IN PARTITION` を使うことで負荷を軽減できます。

`ALTER ... PARTITION` クエリの例は、テスト [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) および [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql) で示されています。