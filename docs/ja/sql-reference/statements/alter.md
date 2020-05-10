---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
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

クエリで、コンマ区切りのアクションのリストを指定します。
各アクションは、列に対する操作です。

次の操作がサポートされます:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column’s type, default expression and TTL.

これらの動作については、以下で詳述する。

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

指定したテーブルに新しい列を追加します `name`, `type`, [`codec`](create.md#codecs) と `default_expr` （セクションを参照 [既定の式](create.md#create-default-values)).

この `IF NOT EXISTS` 句が含まれている場合、列がすでに存在する場合、クエリはエラーを返しません。 指定した場合 `AFTER name_after` （名前のカラムのカラムを追加したものを指定されたもののリストテーブル列あります。 そうしないと、カラムが追加されるのです。 場合がありますので注意してない方の追加カラムの最初に表示します。 アクションの連鎖のために, `name_after` 前のアクションのいずれかで追加される列の名前を指定できます。

列を追加すると、データでアクションを実行せずにテーブル構造が変更されます。 データは後にディスクに表示されません `ALTER`. テーブルから読み取るときに列のデータが欠落している場合は、デフォルト値（デフォルトの式がある場合はデフォルトの式を実行するか、ゼロまたは データパーツをマージした後、ディスク上に列が表示されます [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)).

このアプローチにより、 `ALTER` 古いデータの量を増やすことなく、即座に照会します。

例えば:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

名前の列を削除します `name`. この `IF EXISTS` 句が指定されている場合、列が存在しない場合、クエリはエラーを返しません。

ファイルシステ これはファイル全体を削除するので、クエリはほぼ即座に完了します。

例えば:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

すべてリセットデータ列の指定されたパーティション 詳細設定、パーティションの名前の部 [パーティション式の指定方法](#alter-how-to-specify-part-expr).

この `IF EXISTS` 句が指定されている場合、列が存在しない場合、クエリはエラーを返しません。

例えば:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

列にコメントを追加します。 この `IF EXISTS` 句が指定されている場合、列が存在しない場合、クエリはエラーを返しません。

それぞれの列ができています。 列にコメントが既に存在する場合、新しいコメントは前のコメントを上書きします。

コメントは `comment_expression` によって返される列 [DESCRIBE TABLE](misc.md#misc-describe-table) クエリ。

例えば:

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

この `IF EXISTS` 句が指定されている場合、列が存在しない場合、クエリはエラーを返しません。

タイプを変更すると、値は次のように変換されます。 [toType](../../sql-reference/functions/type-conversion-functions.md) 関数がそれらに適用された。 デフォルトの式だけが変更された場合、クエリは何も複雑ではなく、ほぼ即座に完了します。

例えば:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

いくつかの処理段階があります:

-   変更されたデータを含む一時（新しい）ファイルの準備。
-   古いファイルの名前を変更する。
-   一時(新しい)ファイルの名前を古い名前に変更します。
-   古いファイルを削除する。

最初の段階だけに時間がかかります。 この段階で障害が発生した場合、データは変更されません。
連続したステージのいずれかで障害が発生した場合は、データを手動で復元できます。 古いファイルがファイルシステムから削除されたが、新しいファイルのデータは、ディスクに書き込まれませんでしたし、失われた場合は例外です。

その `ALTER` クエリの変更カラムがそのままに再現されています。 指示はZooKeeperに保存され、各レプリカはそれらを適用します。 すべて `ALTER` クエリは同じ順序で実行されます。 クエリは、他のレプリカで適切なアクションが完了するのを待機します。 ただし、レプリケートされたテーブルの列を変更するクエリは中断され、すべてのアクションは非同期に実行されます。

#### ALTER Queryの制限 {#alter-query-limitations}

その `ALTER` クエリを作成および削除個別要素（カラム）をネストしたデータ構造が全体に入れ子データ構造です。 ネストされたデータ構造を追加するには、次のような名前の列を追加します `name.nested_name` そしてタイプ `Array(T)`. ネストされたデータ構造は、ドットの前に同じ接頭辞を持つ名前を持つ複数の配列列列と同等です。

主キーまたはサンプリングキーの列の削除はサポートされていません。 `ENGINE` 式)。 主キーに含まれる列の型を変更することは、この変更によってデータが変更されない場合にのみ可能です(たとえば、値を列挙型に追加したり、型を変更 `DateTime` に `UInt32`).

この `ALTER` クエリは必要なテーブルの変更を行うのに十分ではありません。 [INSERT SELECT](insert-into.md#insert_query_insert-select) クエリを使用してテーブルを切り替えます。 [RENAME](misc.md#misc_operations-rename) 古いテーブルを照会して削除します。 を使用することができ [クリックハウスコピー機](../../operations/utilities/clickhouse-copier.md) に代わるものとして `INSERT SELECT` クエリ。

その `ALTER` クエリーのブロックすべてを読み込みと書き込んでいます。 言い換えれば、長い場合 `SELECT` の時に動いています `ALTER` クエリ、 `ALTER` クエリはそれが完了するのを待ちます。 同時に、同じテーブルに対するすべての新しいクエリは、 `ALTER` 走ってる

データ自体を格納しないテーブルの場合 `Merge` と `Distributed`), `ALTER` テーブル構造を変更するだけで、下位テーブルの構造は変更されません。 たとえば、ALTERを実行している場合 `Distributed` テーブル、また、実行する必要があります `ALTER` テーブルのすべてすることができます。

### キー式による操作 {#manipulations-with-key-expressions}

以下のコマン:

``` sql
MODIFY ORDER BY new_expression
```

それはの表のためにだけ働きます [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) ファミリー（含む
[複製された](../../engines/table-engines/mergetree-family/replication.md) テーブル）。 このコマンドは、
[ソートキー](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの
に `new_expression` (式または式のタプル)。 主キーは同じままです。

このコマンドは、メタデータのみを変更するという意味で軽量です。 データ部分のプロパティを保持するには
既存の列を含む式を追加することはできません。
ソートキーに(列のみが追加されました。 `ADD COLUMN` 同じでコマンド `ALTER` クエリ）。

### データスキップインデックスの操作 {#manipulations-with-data-skipping-indices}

それはの表のためにだけ働きます [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) ファミリー（含む
[複製された](../../engines/table-engines/mergetree-family/replication.md) テーブル）。 次の操作
利用できます:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` -付加価指数の説明をテーブルメタデータを指すものとします。

-   `ALTER TABLE [db].name DROP INDEX name` -除去す指標の説明からテーブルメタデータを削除を行指数のファイルからディスク。

これらのコマ
また、その複製(同期指標のメタデータを通して飼育係).

### 制約による操作 {#manipulations-with-constraints}

るの詳細を参照してください [制約](create.md#constraints)

次の構文を使用して制約を追加または削除できます:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

クエリに追加または削除約メタデータの制約からテーブルで、速やかに処理します。

制約チェック *実行されません* 既存のデータが追加された場合。

変更後の内容の複製のテーブル放送への飼育係で適用されますその他のレプリカ.

### パーティションとパーツの操作 {#alter_manipulations-with-partitions}

以下の操作 [パーティシ](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 利用できます:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` ディレク
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` テーブルへのディレクトリ。
-   [REPLACE PARTITION](#alter_replace-partition) -データパーティションをテーブル間でコピーします。
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) -コピーするデータを仕切りからテーブルにも置き換え.
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) (\#alter\_move\_to\_table-partition)-あるテーブルから別のテーブルにデータパーティションを移動します。
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

例えば:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

セクションのpartition expressionの設定についての記事を読む [パーティション式の指定方法](#alter-how-to-specify-part-expr).

クエリが実行された後、データを使用して必要な操作を行うことができます `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` すべての複製のディレクトリ。 このクエリはリーダーレプリカでのみ実行できます。 レプリカがリーダーかどうかを調べるには、以下を実行します `SELECT` にクエリ [システム。レプリカ](../../operations/system-tables.md#system_tables-replicas) テーブル。 また、作ることは容易です `DETACH` クエリはすべてのレプリカ-すべてのレプリカ、例外をスロー以外のリーダーレプリカ.

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

削除指定された分割テーブルから. このクエリのタグの仕切りとして休止または消去いたしますデータを完全に約10分です。

セクションのpartition expressionの設定についての記事を読む [パーティション式の指定方法](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

指定したパーティショ `detached`.
セクションのパーティション式の設定の詳細 [パーティション式の指定方法](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

テーブルにデータを追加します。 `detached` ディレクトリ。 パーティション全体または別のパートにデータを追加することができます。 例:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

セクションのパーティション式の設定の詳細 [パーティション式の指定方法](#alter-how-to-specify-part-expr).

このクエリは複製されます。 のレプリカ-イニシエータチェックがあるか否かのデータを `detached` ディレクトリ。 データが存在する場合、クエリは整合性をチェックします。 すべてが正しい場合、クエリはデータをテーブルに追加します。 他のすべてのレプリカをダウンロードからデータのレプリカ-イニシエータです。

したがって、データを `detached` ディレクトリを使用します。 `ALTER ... ATTACH` すべてのレプリカのテーブルにクエリを追加します。

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

このクエリは、データパーティションを `table1` に `table2` のexsistingにデータを加えます `table2`. データは削除されないことに注意してください `table1`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルに同じ構造が必要です。
-   両方の表に同じパーティション-キーが必要です。

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

このクエリは、データパーティションを `table1` に `table2` そしての既存の仕切りを取り替えます `table2`. データは削除されないことに注意してください `table1`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルに同じ構造が必要です。
-   両方の表に同じパーティション-キーが必要です。

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

このクエリは、データパーティションを `table_source` に `table_dest` からデータを削除すると `table_source`.

クエリを正常に実行するには、次の条件を満たす必要があります:

-   両方のテーブルに同じ構造が必要です。
-   両方の表に同じパーティション-キーが必要です。
-   両方のテーブルと同じでなければならエンジンです。 (複製または非レプリケート)
-   両方の表に同じストレージポリシーが必要です。

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

パーティショ この `DEFAULT` このクエリは、指定されたデフォルト値に列の値を設定し、テーブルを作成するときに句が決定された。

例えば:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

このクエ この `PARTITION` 条項を省略して、クエリーを作成し、バックアップの仕切ります。

!!! note "メモ"
    バックアップ処理全体は、サーバーを停止せずに実行されます。

古いスタイルのテーブルでは、パーティション名のプレフィックスを指定できます(例, ‘2019’)のクエリーを作成し、バックアップのためのすべてに対応する隔壁 セクションのpartition expressionの設定についての記事を読む [パーティション式の指定方法](#alter-how-to-specify-part-expr).

実行時に、データスナップショットの場合、クエリはテーブルデータへのハードリンクを作成します。 ディレクト `/var/lib/clickhouse/shadow/N/...`、どこ:

-   `/var/lib/clickhouse/` 設定で指定されたClickHouseの作業ディレクトリです。
-   `N` バックアップの増分数です。

!!! note "メモ"
    使用する場合 [テーブル内のデータストレージのディスクのセット](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)、を `shadow/N` ディレクトリが表示される毎にディスクデータを格納する部品と合わせによる `PARTITION` 式。

同じディレクトリ構造がバックアップ内に作成されます。 `/var/lib/clickhouse/`. クエリが実行されます ‘chmod’ すべてのファイルについて、それらへの書き込みを禁止。

バックアップを作成したら、次のデータをコピーできます `/var/lib/clickhouse/shadow/` リモートサーバーに移動し、ローカルサーバーから削除します。 それに注意しなさい `ALTER t FREEZE PARTITION` クエリは複製されません。 するための地元のバックアップ、現地サーバーです。

クエリをバックアップトで最初のでお待ちしておりますので、現在のクエリーに対応するテーブルに仕上げた。

`ALTER TABLE t FREEZE PARTITION` コピーのみのデータのないテーブルメタデータを指すものとします。 をバックアップテーブルメタデータ、コピー、ファイル `/var/lib/clickhouse/metadata/database/table.sql`

バックアップからデータを復元するには:

1.  テーブルが存在しない場合はテーブルを作成します。 クエリを表示するには、を使用します。sqlファイル(置換 `ATTACH` それで `CREATE`).
2.  からデータをコピーします `data/database/table/` バックアップの中のディレクトリ `/var/lib/clickhouse/data/database/table/detached/` ディレクトリ。
3.  走れ。 `ALTER TABLE t ATTACH PARTITION` データをテーブルに追加するクエリ。

バックア

バックアップおよびデータの復元の詳細については、次を参照 [データバック](../../operations/backup.md) セクション。

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

クエリは次のように動作します `CLEAR COLUMN` しかし、列データの代わりに索引をリセットします。

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

ダウンロードパーティションから別のサーバーです。 このクエリーだけを再現します。

クエリは次の処理を行います:

1.  指定したシャードからパーティションをダウ で ‘path-in-zookeeper’ ZooKeeperでシャードへのパスを指定する必要があります。
2.  次に、クエリはダウンロードされたデータを `detached` のディレクトリ `table_name` テーブル。 を使用 [ATTACH PARTITION\|PART](#alter_attach-partition) データをテーブルに追加するためのクエリ。

例えば:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

それに注意:

-   その `ALTER ... FETCH PARTITION` クエリは複製されません。 それはに仕切りを置きます `detached` ディレクト
-   その `ALTER TABLE ... ATTACH` クエリが複製されます。 すべてのレプリカにデータを追加します。 データは、次のいずれかのレプリカに追加されます。 `detached` ディレクトリ、および他の人に-近隣のレプリカから。

ダウンロードする前に、システムかどうかをチェックすると、パーティションが存在するとテーブル構造。 最も適切なレプリカは、正常なレプリカから自動的に選択されます。

クエリは呼び出されますが `ALTER TABLE` テーブル構造は変更されず、テーブルで使用できるデータもすぐには変更されません。

#### MOVE PARTITION\|PART {#alter_move-partition}

別のボリュームまたはディ `MergeTree`-エンジンテーブル。 見る [複数ブロックデバイスを使用したデータ保存](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

その `ALTER TABLE t MOVE` クエリ:

-   な再現が異なるレプリカで保管。
-   指定されたディスクまたはボリ また、ストレージポリシーで指定されたデータ移動の条件を適用できない場合は、エラーが返されます。
-   移動するデータがバックグラウンドプロセスによって既に移動されている場合にエラーを返すことができます。 `ALTER TABLE t MOVE` クエリとして結果データの統合. この場合、ユーザーは追加の操作を行うべきではありません。

例えば:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### パーティション式の設定方法 {#alter-how-to-specify-part-expr}

パーティション式を指定するには `ALTER ... PARTITION` 異なる方法でクエリ:

-   からの値として `partition` の列 `system.parts` テーブル。 例えば, `ALTER TABLE visits DETACH PARTITION 201901`.
-   テーブル列からの式として。 定数と定数式がサポートされています。 例えば, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   パーティションidの使用。 パーティションidは、ファイルシステムおよびzookeeper内のパーティションの名前として使用されるパーティションの文字列識別子です(可能であれば、人間が パーティションidを指定する必要があります。 `PARTITION ID` 一重引quotesでの句。 例えば, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   で [ALTER ATTACH PART](#alter_attach-partition) と [DROP DETACHED PART](#alter_drop-detached) クエリ、パートの名前を指定するには、文字列リテラルを使用します。 `name` の列 [システム。detached\_parts](../../operations/system-tables.md#system_tables-detached_parts) テーブル。 例えば, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

ご利用の引用符を指定する場合、パーティションのエントランスは目を引く壁面緑化を表現。 たとえば、 `String` その名前を引用符で指定する必要があります (`'`). のための `Date` と `Int*` タイプ引用符は必要ありません。

古いスタイルのテーブルの場合は、パーティションを数値として指定できます `201901` または文字列 `'201901'`. 新しいスタイルのテーブルの構文は、型が厳しくなります（VALUES入力フォーマットのパーサーと同様）。

上記のすべてのルールは、 [OPTIMIZE](misc.md#misc_operations-optimize) クエリ。 を指定する場合にのみ分配時の最適化、非仕切られたテーブルセットの表現 `PARTITION tuple()`. 例えば:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

の例 `ALTER ... PARTITION` クエリはテストで実証されています [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) と [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### テーブルttlによる操作 {#manipulations-with-table-ttl}

変更することができ [テーブルTTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) 次のフォームの要求で:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### ALTERクエリのシンクロニシティ {#synchronicity-of-alter-queries}

非複製可能なテーブルの場合は、すべて `ALTER` クエリは同期的に実行されます。 のためのreplicatableテーブル、クエリーだけで追加指示のための適切な行動を `ZooKeeper`、そしてアクション自体はできるだけ早く実行されます。 しかし、クエリーが待機するためにこれらの行動は完了するすべてのレプリカ.

のために `ALTER ... ATTACH|DETACH|DROP` クエリを使用することができます `replication_alter_partitions_sync` 待ちを設定する設定。
可能な値: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### 突然変異 {#alter-mutations}

突然変異は、テーブル内の行を変更または削除できるようにするalter query variantです。 標準とは対照的に `UPDATE` と `DELETE` ポイントデータの変更を目的としたクエリ、突然変異は、テーブル内の多くの行を変更する重い操作を目的としています。 のために支えられる `MergeTree` 家族のテーブルエンジンなどのエンジンの複製です。

既存のテーブルはそのまま変異可能です（変換は必要ありません）が、最初の変更がテーブルに適用されると、そのメタデータ形式は以前のサーバーバージョンと

現在使用可能なコマンド:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

その `filter_expr` 型でなければならな `UInt8`. クエリは、この式がゼロ以外の値をとるテーブルの行を削除します。

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

その `filter_expr` 型でなければならな `UInt8`. このクエリは、指定された列の値を、対応する式の値に更新します。 `filter_expr` ゼロ以外の値をとります。 値は列タイプにキャストされます。 `CAST` オペレーター プライマリキーまたはパーティションキーの計算で使用される列の更新はサポートされません。

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

クエリを再建の二次指数 `name` パーティション内 `partition_name`.

一つのクエリを含むことができ複数のコマンドをカンマで区切られています。

用\*mergetreeテーブル突然変異の実行による書き換え全体のデータ部品です。 atomicity-部品は準備ができているおよびaとすぐ変異する部品の代わりになりますありません `SELECT` 変異中に実行を開始したクエリには、まだ変更されていない部分のデータと共に既に変更されている部分のデータが表示されます。

突然変異は、作成順序によって完全に順序付けられ、その順序で各パートに適用されます。 突然変異が提出される前にテーブルに挿入されたデータは突然変異され、その後に挿入されたデータは突然変異されません。 この変異のないブロックを挿入します。

変更クエリは、変更エントリが追加された直後に返されます（レプリケートされたテーブルがzookeeperにある場合、非レプリケートされたテーブルがファイルシス の突然変異体の執行を非同利用システムの概要を設定します。 突然変異の進行状況を追跡するには、 [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) テーブル。 正常に送信された変更は、ClickHouseサーバーが再起動されても引き続き実行されます。 それが提出されると突然変異をロールバックする方法はありませんが、何らかの理由で突然変異が起こった場合、それをキャンセルすることができ [`KILL MUTATION`](misc.md#kill-mutation) クエリ。

終了した突然変異のためのエントリはすぐに削除されません（保存されたエントリの数は `finished_mutations_to_keep` ストレージエンジン変数）。 古い変異エントリが削除されます。

[元の記事](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
