---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

その `ALTER` クエリーのみ対応して `*MergeTree` テーブルだけでなく、 `Merge`と`Distributed`. クエリに複数のバリエーションがあります。

### 列の操作 {#column-manipulations}

テーブル構造の変更。

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

クエリでは、コンマ区切りのアクションのリストを指定します。
各アクションは、列に対する操作です。

次の操作がサポートされます:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column's type, default expression and TTL.

これらの行動を詳細に説明します。

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

指定されたテーブルに新しい列を追加します `name`, `type`, [`codec`](create.md#codecs) と `default_expr` （節を参照 [既定の式](create.md#create-default-values)).

もし `IF NOT EXISTS` 列が既に存在する場合、クエリはエラーを返しません。 指定した場合 `AFTER name_after` （別の列の名前）、列は表の列のリスト内で指定された列の後に追加されます。 それ以外の場合は、列がテーブルの最後に追加されます。 テーブルの先頭に列を追加する方法はないことに注意してください。 一連の行動のために, `name_after` 前の操作のいずれかで追加された列の名前を指定できます。

列を追加すると、データでアクションを実行せずにテーブル構造が変更されます。 その後、データはディスクに表示されません `ALTER`. テーブルから読み取るときに列のデータがない場合は、デフォルト値が入力されます（デフォルトの式がある場合はデフォルトの式を実行するか、ゼロ 列の表示のディスクと統合データ部品（ [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md)).

このアプローチにより、 `ALTER` 古いデータの量を増やすことなく、瞬時にクエリ。

例:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

名前の列を削除します `name`. もし `IF EXISTS` 列が存在しない場合、クエリはエラーを返しません。

コンピュータのデータを削除するファイルシステム。 この削除全ファイル、クエリーがほぼ完了します。

例:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

すべてリセットデータ列の指定されたパーティション セクションのパーティション名の設定の詳細を読む [パーティション式の指定方法](#alter-how-to-specify-part-expr).

もし `IF EXISTS` 列が存在しない場合、クエリはエラーを返しません。

例:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

列にコメントを追加します。 もし `IF EXISTS` 列が存在しない場合、クエリはエラーを返しません。

各列には一つのコメントがあります。 列にコメントが既に存在する場合、新しいコメントは前のコメントを上書きします。

コメントは `comment_expression` によって返される列 [DESCRIBE TABLE](misc.md#misc-describe-table) クエリ。

例:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

このクエリは、 `name` 列のプロパティ:

-   タイプ

-   既定の式

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

もし `IF EXISTS` 列が存在しない場合、クエリはエラーを返しません。

型を変更するとき、値は次のように変換されます [トタイプ](../../sql-reference/functions/type-conversion-functions.md) 機能をそれらに適用した。 既定の式のみが変更された場合、クエリは複雑な処理を行わず、ほぼ即座に完了します。

例:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

いくつかの処理段階があります:

-   準備一時(新しいファイルが修正データです。
-   古いファイルの名前を変更する。
-   一時(新しい)ファイルの名前を古い名前に変更します。
-   古いファイルを削除します。

最初の段階だけ時間がかかります。 この段階で障害が発生した場合、データは変更されません。
連続するいずれかの段階で障害が発生した場合、データを手動で復元することができます。 例外は、古いファイルがファイルシステムから削除されたが、新しいファイルのデータがディスクに書き込まれず、失われた場合です。

その `ALTER` 列を変更するクエリが複製されます。 命令はZooKeeperに保存され、各レプリカはそれらを適用します。 すべて `ALTER` クエリは同じ順序で実行されます。 クエリは、他のレプリカで適切なアクションが完了するのを待機します。 ただし、レプリケートされたテーブル内の列を変更するクエリは中断され、すべてのアクションが非同期に実行されます。

#### クエリの制限の変更 {#alter-query-limitations}

その `ALTER` クエリを作成および削除個別要素（カラム）をネストしたデータ構造が全体に入れ子データ構造です。 入れ子になったデータ構造を追加するには、次のような名前の列を追加できます `name.nested_name` そしてタイプ `Array(T)`. 入れ子になったデータ構造は、ドットの前に同じプレフィックスを持つ名前を持つ複数の配列列と同等です。

主キーまたはサンプリングキーの列を削除することはサポートされていません。 `ENGINE` 式）。 主キーに含まれる列の型を変更することは、この変更によってデータが変更されない場合にのみ可能です(たとえば、列挙型に値を追加したり、型を変更 `DateTime` に `UInt32`).

もし `ALTER` クエリでは、必要なテーブルの変更を行うのに十分ではありません。 [INSERT SELECT](insert-into.md#insert_query_insert-select) を使用してテーブルを切り替えます [RENAME](misc.md#misc_operations-rename) 古いテーブルを照会して削除します。 を使用することができます [クリックハウス-複写機](../../operations/utilities/clickhouse-copier.md) の代わりとして `INSERT SELECT` クエリ。

その `ALTER` クエリーのブロックすべてを読み込みと書き込んでいます。 言い換えれば、 `SELECT` の時に実行されている `ALTER` クエリは、 `ALTER` クエリは、それが完了するのを待ちます。 同時に、同じテーブルに対するすべての新しいクエリは、この間待機します `ALTER` 走ってる

データ自体を格納しないテーブルの場合(以下のように `Merge` と `Distributed`), `ALTER` テーブル構造を変更するだけで、下位テーブルの構造は変更されません。 たとえば、 `Distributed` テーブル、あなたも実行する必要があります `ALTER` テーブルのすべてすることができます。

### キー式による操作 {#manipulations-with-key-expressions}

次のコマン:

``` sql
MODIFY ORDER BY new_expression
```

これは、 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 家族（含む
[複製](../../engines/table-engines/mergetree-family/replication.md) テーブル）。 このコマンドは
[ソートキー](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの
に `new_expression` (式または式のタプル)。 主キーは同じままです。

このコマンドは、メタデータのみを変更するという意味で軽量です。 データ部分のプロパティを保持するには
行は並べ替えキー式で並べ替えられます既存の列を含む式は追加できません
によって追加された列のみ `ADD COLUMN` 同じコマンド `ALTER` クエリ）。

### 操作データを飛指標 {#manipulations-with-data-skipping-indices}

これは、 [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 家族（含む
[複製](../../engines/table-engines/mergetree-family/replication.md) テーブル）。 次の操作
利用できます:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` -付加価指数の説明をテーブルメタデータを指すものとします。

-   `ALTER TABLE [db].name DROP INDEX name` -除去す指標の説明からテーブルメタデータを削除を行指数のファイルからディスク。

これらのコマンドは軽量でいるという意味においてのみ変化メタデータの削除ファイルです。
また、それらは複製されます（ZooKeeperを介して索引メタデータを同期）。

### 制約による操作 {#manipulations-with-constraints}

詳細はこちら [制約](create.md#constraints)

制約は、次の構文を使用して追加または削除できます:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

クエリに追加または削除約メタデータの制約からテーブルで、速やかに処理します。

制約チェック *実行されません* 既存のデータが追加された場合。

変更後の内容の複製のテーブル放送への飼育係で適用されますその他のレプリカ.

### パーティションとパーツの操作 {#alter_manipulations-with-partitions}

次の操作は [仕切り](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 利用できます:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` ディレクトリとそれを忘れ。
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` テーブルへのディレクトリ。
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) -コピーするデータを仕切りからテーブルにも置き換え.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition)(#alter_move_to_table-partition)-データ-パーティションをあるテーブルから別のテーブルに移動します。
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) -パーティション内の指定された列の値をリセットします。
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) -リセットの指定された二次インデックス、パーティション
-   [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) – Move partition/data part to another disk or volume.

<!-- -->

#### DETACH PARTITION {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

指定されたパーティションのすべてのデータを `detached` ディレクトリ。 サーバーのを忘れているのは、一戸建てのデータを分配していない場合は存在します。 サーバーはこのデータについて知りません。 [ATTACH](#alter_attach-partition) クエリ。

例:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

セクションでのパーティション式の設定について [パーティション式の指定方法](#alter-how-to-specify-part-expr).

クエリが実行された後、データを使用して好きなことを行うことができます。 `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` すべてのレプリカ上の このクエリは、リーダーレプリカでのみ実行できます。 する場合は、レプリカは、オーソドックスなアプローチを行う `SELECT` クエリを実行する [システムレプリカ](../../operations/system-tables.md#system_tables-replicas) テーブル。 あるいは、 `DETACH` クエリはすべてのレプリカ-すべてのレプリカ、例外をスロー以外のリーダーレプリカ.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

指定したパーティションを表から削除します。 このクエリのタグの仕切りとして休止または消去いたしますデータを完全に約10分です。

セクションでのパーティション式の設定について [パーティション式の指定方法](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

指定された部分または指定されたパーティションのすべての部分を `detached`.
セクションでのパーティション式の設定の詳細 [パーティション式の指定方法](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

テーブルにデータを追加します。 `detached` ディレクトリ。 パーティション全体または別の部分のデータを追加することができます。 例:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

セクションでのパーティション式の設定の詳細 [パーティション式の指定方法](#alter-how-to-specify-part-expr).

このクエリは複製されます。 のレプリカ-イニシエータチェックがあるか否かのデータを `detached` ディレクトリ。 データが存在する場合、クエリはその整合性を確認します。 すべてが正しい場合、クエリはデータをテーブルに追加します。 他のすべてのレプリカをダウンロードからデータのレプリカ-イニシエータです。

だから、データを置くことができます `detached` ディレクトリを一つのレプリカ上に置き、 `ALTER ... ATTACH` すべてのレプリカのテーブルに追加するクエリ。

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

このクエリは、データパーティションを `table1` に `table2` にデータを追加します。 `table2`. データは削除されないことに注意してください `table1`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルを作成するときに必要となる構造です。
-   両方のテーブルを作成するときに必要となる分割。

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

このクエリは、データパーティションを `table1` に `table2` の既存のパーティションを置き換え `table2`. データは削除されないことに注意してください `table1`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルを作成するときに必要となる構造です。
-   両方のテーブルを作成するときに必要となる分割。

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

このクエリは、データパーティションを `table_source` に `table_dest` からデータを削除すると `table_source`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルを作成するときに必要となる構造です。
-   両方のテーブルを作成するときに必要となる分割。
-   両方のテーブルと同じでなければならエンジンです。 （複製または非複製)
-   両方のテーブルを作成するときに必要となる貯ます。

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

すべてリセット値で指定されたカラムがありました。 もし `DEFAULT` 句が決定されたテーブルを作成するときに、このクエリは、指定された既定値に列の値を設定します。

例:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

このクエリーを作成し、地元のバックアップの指定されたパーティション もし `PARTITION` 条項を省略して、クエリーを作成し、バックアップの仕切ります。

!!! note "注"
    バックアップ処理全体は、サーバーを停止せずに実行されます。

古いスタイルのテーブルの場合、パーティション名のプレフィックスを指定できます(例えば, ‘2019’)のクエリーを作成し、バックアップのためのすべてに対応する隔壁 セクションでのパーティション式の設定について [パーティション式の指定方法](#alter-how-to-specify-part-expr).

実行時に、データスナップショットの場合、クエリはテーブルデータへのハードリンクを作成します。 Hardlinksに設置されているディレクトリ `/var/lib/clickhouse/shadow/N/...`,ここで:

-   `/var/lib/clickhouse/` 設定で指定された作業ClickHouseディレクトリです。
-   `N` バックアップの増分数です。

!!! note "注"
    を使用する場合 [テーブル内のデータ格納用のディスクのセット](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) は、 `shadow/N` ディレクトリが表示される毎にディスクデータを格納する部品と合わせによる `PARTITION` 式。

バックアップの内部と同じディレクトリ構造が作成されます `/var/lib/clickhouse/`. クエリの実行 ‘chmod’ すべてのファイルの禁止に対して書き込みます。

バックアップの作成後、データをコピーするには `/var/lib/clickhouse/shadow/` ローカルサーバーから削除します。 なお、 `ALTER t FREEZE PARTITION` クエリは複製されません。 するための地元のバックアップ、現地サーバーです。

クエリをバックアップトで最初のでお待ちしておりますので、現在のクエリーに対応するテーブルに仕上げた。

`ALTER TABLE t FREEZE PARTITION` コピーのみのデータのないテーブルメタデータを指すものとします。 をバックアップテーブルメタデータ、コピー、ファイル `/var/lib/clickhouse/metadata/database/table.sql`

バックアップからデータを復元するには:

1.  テーブルが存在しない場合はテーブルを作成します。 クエリを表示するには、を使用します。sqlファイル(置換 `ATTACH` それで `CREATE`).
2.  からデータをコピーします `data/database/table/` バックアップ内のディレクトリ `/var/lib/clickhouse/data/database/table/detached/` ディレクトリ。
3.  走れ。 `ALTER TABLE t ATTACH PARTITION` テーブルにデータを追加するクエリ。

復元からのバックアップを必要としないの停止、サーバーにコピーします。

バックアップおよびデータの復元の詳細については、 [データバックア](../../operations/backup.md) セクション

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

クエリは次のように動作します `CLEAR COLUMN` しかし、列データではなくインデックスをリセットします。

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

ダウンロードパーティションから別のサーバーです。 このクエリーだけを再現します。

クエリは、次の操作を実行します:

1.  ダウンロードパーティションから、指定されたザ-シャー. で ‘path-in-zookeeper’ ZooKeeperでシャードへのパスを指定する必要があります。
2.  次に、クエリはダウンロードしたデータを `detached` のディレクトリ `table_name` テーブル。 使用する [ATTACH PARTITION\|PART](#alter_attach-partition) テーブルにデータを追加するクエリ。

例えば:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

なお:

-   その `ALTER ... FETCH PARTITION` クエリは複製されません。 それは仕切りをに置きます `detached` ディレクトリの現地サーバーです。
-   その `ALTER TABLE ... ATTACH` クエリが複製されます。 すべてのレプリカにデータを追加します。 データはレプリカのいずれかに追加されます。 `detached` ディレクトリ、および他の人に-隣接するレプリカから。

ダウンロードする前に、システムかどうかをチェックすると、パーティションが存在するとテーブル構造。 最も適切なレプリカは、正常なレプリカから自動的に選択されます。

クエリは呼び出されますが `ALTER TABLE` テーブル構造を変更せず、テーブル内で使用可能なデータを直ちに変更することはありません。

#### MOVE PARTITION\|PART {#alter_move-partition}

パーティションまた `MergeTree`-エンジンテーブル。 見る [複数のブロックデバイスのためのデータ保存](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

その `ALTER TABLE t MOVE` クエリ:

-   な再現が異なるレプリカで保管。
-   指定されたディスクまたはボリュ ストレージポリシーで指定されたデータ移動の条件を適用できない場合、Queryはエラーを返します。
-   復帰できるエラーの場合、データの移動に移行している背景には、同時 `ALTER TABLE t MOVE` クエリまたはバックグラウンドデータマージの結果。 この場合、ユーザーは追加の操作を実行しないでください。

例:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### パーティション式の設定方法 {#alter-how-to-specify-part-expr}

パーティション式を指定するには `ALTER ... PARTITION` 異なる方法でのクエリ:

-   からの値として `partition` の列 `system.parts` テーブル。 例えば, `ALTER TABLE visits DETACH PARTITION 201901`.
-   テーブル列からの式として。 定数と定数式がサポートされています。 例えば, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   パーティションIDの使用。 Partition IDは、ファイルシステムおよびZooKeeperのパーティションの名前として使用されるパーティションの文字列識別子(可能であれば人間が読める)です。 パーティションIDは、 `PARTITION ID` 一重引quotesで囲まれた句。 例えば, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   で [ALTER ATTACH PART](#alter_attach-partition) と [DROP DETACHED PART](#alter_drop-detached) パーツの名前を指定するには、文字列リテラルを使用します。 `name` の列 [システムdetached_parts](../../operations/system-tables.md#system_tables-detached_parts) テーブル。 例えば, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

ご利用の引用符を指定する場合、パーティションのエントランスは目を引く壁面緑化を表現。 例えば、 `String` その名前を引用符で指定する必要があります (`'`). のために `Date` と `Int*` 型引用符は必要ありません。

古いスタイルのテーブルの場合は、パーティションを数値として指定できます `201901` または文字列 `'201901'`. 新しいスタイルのテーブルの構文は、型によってより厳密になります（値の入力形式のパーサーと同様）。

上記のすべてのルールは、 [OPTIMIZE](misc.md#misc_operations-optimize) クエリ。 を指定する場合にのみ分配時の最適化、非仕切られたテーブルセットの表現 `PARTITION tuple()`. 例えば:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

の例 `ALTER ... PARTITION` クエリは、テストで示されます [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) と [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### テーブルTTLによる操作 {#manipulations-with-table-ttl}

変更できます [テーブルTTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) 以下のフォームのリクエストで:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### ALTERクエリの同期性 {#synchronicity-of-alter-queries}

複製不可能なテーブルの場合、すべて `ALTER` クエリは同期的に実行されます。 のためのreplicatableテーブル、クエリーだけで追加指示のための適切な行動を `ZooKeeper` そして、アクション自体はできるだけ早く実行されます。 ただし、クエリは、すべてのレプリカでこれらの操作が完了するまで待機できます。

のために `ALTER ... ATTACH|DETACH|DROP` クエリを使用することができます `replication_alter_partitions_sync` 待機を設定する設定。
可能な値: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### 突然変異 {#alter-mutations}

突然変異は、テーブル内の行を変更または削除できるALTER query variantです。 標準とは対照的に `UPDATE` と `DELETE` ポイントデータの変更、突然変異を目的としたクエリは、テーブル内の多くの行を変更する重い操作を目的としています。 のために支えられる `MergeTree` 家族のテーブルエンジンなどのエンジンの複製です。

既存のテーブルはそのままで(変換は必要ありません)突然変異の準備ができていますが、最初の突然変異がテーブルに適用されると、そのメタデータ形式

現在利用可能なコマンド:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

その `filter_expr` 型である必要があります `UInt8`. このクエリは、この式がゼロ以外の値をとるテーブル内の行を削除します。

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

その `filter_expr` 型である必要があります `UInt8`. このクエリは、指定された列の値を、指定された列の行の対応する式の値に更新します。 `filter_expr` ゼロ以外の値をとります。 値は、列型にキャストされます。 `CAST` オペレーター プライマリキーまたはパーティションキーの計算で使用される列の更新はサポートされません。

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

クエリを再建の二次指数 `name` パーティション内 `partition_name`.

一つのクエリを含むことができ複数のコマンドをカンマで区切られています。

\*MergeTree表の場合、突然変異はデータ部分全体を書き換えることによって実行されます。 原子性がない-部品は準備ができているとすぐ変異させた部品のために置き換えられ、a `SELECT` 突然変異中に実行を開始したクエリには、すでに変異している部分のデータと、まだ変異していない部分のデータが表示されます。

突然変異は完全にそれらの作成順序によって順序付けられ、その順序で各部分に適用される。 突然変異が送信される前にテーブルに挿入されたデータは変異され、その後に挿入されたデータは変異されません。 突然変異は挿入を決してブロックしないことに注意してください。

突然変異クエリは、突然変異エントリが追加された直後に返されます(複製されたテーブルがZooKeeperに、複製されていないテーブルがファイルシステムに)。 の突然変異体の執行を非同利用システムの概要を設定します。 あなたが使用することができ、突然変異の進行を追跡するために [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) テーブル。 正常に送信された突然変異は、ClickHouseサーバーが再起動されても実行され続けます。 それが提出された後、突然変異をロールバックする方法はありませんが、突然変異が何らかの理由で立ち往生している場合、それは [`KILL MUTATION`](misc.md#kill-mutation) クエリ。

完了した突然変異のエントリはすぐに削除されません（保存されたエントリの数は、 `finished_mutations_to_keep` ストレージエンジ 古い突然変異エントリは削除されます。

## ALTER USER {#alter-user-statement}

ClickHouseユーザーアカウントの変更。

### 構文 {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### 説明 {#alter-user-dscr}

使用するには `ALTER USER` あなたは、 [ALTER USER](grant.md#grant-access-management) 特権だ

### 例 {#alter-user-examples}

付与されたロールを既定に設定する:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

ロールが以前にユーザーに付与されていない場合、ClickHouseは例外をスローします。

付与されたすべての役割をdefaultに設定します:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

将来ユーザーにロールが付与されると、自動的にデフォルトになります。

セットの付与の役割をデフォルトを除く `role1` と `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

## ALTER ROLE {#alter-role-statement}

役割を変更します。

### 構文 {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## ALTER ROW POLICY {#alter-row-policy-statement}

行ポリシーを変更します。

### 構文 {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER QUOTA {#alter-quota-statement}

クォータを変更します。

### 構文 {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

クォータを変更します。

### 構文 {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[元の記事](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
