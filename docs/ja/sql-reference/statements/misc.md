---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u305D\u306E\u4ED6"
---

# その他のクエリ {#miscellaneous-queries}

## ATTACH {#attach}

このクエリはまったく同じです `CREATE` でも

-   言葉の代わりに `CREATE` それは単語を使用します `ATTACH`.
-   クエリはディスク上にデータを作成するのではなく、データがすでに適切な場所にあると仮定し、テーブルに関する情報をサーバーに追加するだけです。
    アタッチクエリを実行すると、サーバーはテーブルの存在を知ります。

テーブルが以前にデタッチされた場合 (`DETACH`）、その構造が知られていることを意味する、あなたは構造を定義せずに省略形を使用することができます。

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

このクエリは、サーバーの起動時に使用されます。 サーバーに店舗のテーブルメタデータとしてファイル `ATTACH` 単に起動時に実行されるクエリ（サーバー上で明示的に作成されるシステムテーブルを除く）。

## CHECK TABLE {#check-table}

表のデータが破損しているかどうかを確認します。

``` sql
CHECK TABLE [db.]name
```

その `CHECK TABLE` queryは、実際のファイルサイズと、サーバーに格納されている期待値を比較します。 ファイルサイズが格納された値と一致しない場合は、データが破損していることを意味します。 このが発生する可能性があります、例えば、システムがクラッシュ時のクエリを実行します。

クエリ応答には、 `result` 単一行の列。 行の値は次のとおりです
[ブール値](../../sql-reference/data-types/boolean.md) タイプ:

-   0-テーブル内のデータが破損しています。
-   1-データは整合性を維持します。

その `CHECK TABLE` クエリは以下のテーブルエンジン:

-   [ログ](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [ストリップログ](../../engines/table-engines/log-family/stripelog.md)
-   [メルゲツリー族](../../engines/table-engines/mergetree-family/mergetree.md)

これは、テーブルが別のテーブルエンジンの原因となる例外です。

からのエンジン `*Log` 家族は失敗の自動データ回復を提供しない。 使用する `CHECK TABLE` タイムリーにデータ損失を追跡するためのクエリ。

のために `MergeTree` ファミリーエンジン `CHECK TABLE` クエリを示すステータス確認のための個人データのテーブルに現地サーバーです。

**データが破損している場合**

テーブルが破損している場合は、破損していないデータを別のテーブルにコピーできます。 これを行うには:

1.  破損したテーブルと同じ構造を持つ新しいテーブルを作成します。 これを行うにはクエリを実行します `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  セット [max\_threads](../../operations/settings/settings.md#settings-max_threads) 単一のスレッドで次のクエリを処理するには、値を1に設定します。 このクエリ `SET max_threads = 1`.
3.  クエリの実行 `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. この要求により、破損していないデータが破損した表から別の表にコピーされます。 破損した部分の前のデータのみがコピーされます。
4.  再起動 `clickhouse-client` リセットするには `max_threads` 値。

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

次の値を返します `String` タイプ列:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [既定の式](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` または `ALIAS`). 既定の式が指定されていない場合、Columnには空の文字列が含まれます。
-   `default_expression` — Value specified in the `DEFAULT` 句。
-   `comment_expression` — Comment text.

入れ子になったデータ構造は “expanded” 形式。 各列は、ドットの後に名前を付けて別々に表示されます。

## DETACH {#detach}

に関する情報を削除します。 ‘name’ サーバーからのテーブル。 サーバーは、テーブルの存在を知ることを停止します。

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

テーブルのデータまたはメタデータは削除されません。 次のサーバー起動時に、サーバーはメタデータを読み取り、テーブルについて再度確認します。
同様に、 “detached” テーブルはを使用して再付すことができます `ATTACH` クエリ(メタデータが格納されていないシステムテーブルを除く)。

ありません `DETACH DATABASE` クエリ。

## DROP {#drop}

このクエリ慮して、調教メニューを組み立て: `DROP DATABASE` と `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

内部のすべてのテーブルを削除 ‘db’ データベースを削除します。 ‘db’ データベース自体。
もし `IF EXISTS` データベースが存在しない場合、エラーは返されません。

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

テーブルを削除します。
もし `IF EXISTS` テーブルが存在しない場合、またはデータベースが存在しない場合、エラーは返されません。

    DROP DICTIONARY [IF EXISTS] [db.]name

辞書を削除します。
もし `IF EXISTS` テーブルが存在しない場合、またはデータベースが存在しない場合、エラーは返されません。

## DROP USER {#drop-user-statement}

ユーザーを削除します。

### 構文 {#drop-user-syntax}

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

ロールを削除します。

削除された役割は、付与されたすべてのエンティティから取り消されます。

### 構文 {#drop-role-syntax}

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

行ポリシーを削除します。

削除行の政策が取り消すべての主体で割り当てられます。

### 構文 {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

クォータを削除します。

削除枠が取り消すべての主体で割り当てられます。

### 構文 {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

クォータを削除します。

削除枠が取り消すべての主体で割り当てられます。

### 構文 {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

単一を返します `UInt8`-単一の値を含む列を入力します `0` テーブルまたはデータベースが存在しない場合、または `1` 指定されたデータベースにテーブルが存在する場合。

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

現在実行中のクエリを強制的に終了しようとします。
終了するクエリがシステムから選択されます。で定義された基準を使用してテーブルを処理します `WHERE` の節 `KILL` クエリ。

例:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

読み取り専用ユーザーは、独自のクエリのみを停止できます。

既定では、非同期バージョンのクエリが使用されます (`ASYNC`)、クエリが停止したことの確認を待たない。

同期バージョン (`SYNC`)すべてのクエリが停止するのを待機し、停止すると各プロセスに関する情報を表示します。
応答には、 `kill_status` 列は、次の値を取ることができます:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

テストクエリ (`TEST`)ユーザーの権限のみをチェックし、停止するクエリのリストを表示します。

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

取り消しと削除を試みます [突然変異](alter.md#alter-mutations) 現在実行中です 取り消すべき突然変異はから選ばれます [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) によって指定されたフィルタを使用する表 `WHERE` の節 `KILL` クエリ。

テストクエリ (`TEST`)ユーザーの権限のみをチェックし、停止するクエリのリストを表示します。

例:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

突然変異によって既に行われた変更はロールバックされません。

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

このクエリは、テーブルエンジンを使用してテーブルのデータ部分の予定外のマージを初期化しようとします。 [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md) 家族だ

その `OPTMIZE` クエリは、 [マテリアライズドビュー](../../engines/table-engines/special/materializedview.md) そして、 [バッファ](../../engines/table-engines/special/buffer.md) エンジンだ その他のテーブルエンジンなサポート。

とき `OPTIMIZE` と共に使用されます [複製マージツリー](../../engines/table-engines/mergetree-family/replication.md) テーブルエンジンのファミリでは、ClickHouseはマージ用のタスクを作成し、すべてのノードで実行を待機します。 `replication_alter_partitions_sync` 設定が有効になっています）。

-   もし `OPTIMIZE` 何らかの理由でマージを実行せず、クライアントに通知しません。 通知を有効にするには、 [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) 設定。
-   を指定した場合 `PARTITION` 指定したパーティションのみが最適化されます。 [パーティション式の設定方法](alter.md#alter-how-to-specify-part-expr).
-   指定した場合 `FINAL`、最適化は、すべてのデータが一つの部分に既にある場合でも実行されます。
-   指定した場合 `DEDUPLICATE` その後、完全に同一の行が重複除外されます（すべての列が比較されます）。

!!! warning "警告"
    `OPTIMIZE` できない修正 “Too many parts” エラー

## RENAME {#misc_operations-rename}

テーブルの名前を変更します。

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

すべてのテーブル名変更"グローバルチェンジにおけるロックしなければなりません。 テーブルの名前を変更することは簡単な操作です。 TOの後に別のデータベースを指定した場合、表はこのデータベースに移動されます。 しかし、そのディレクトリのデータベースに格納してある必要がある同一ファイルシステム(それ以外の場合、エラーを返す。

## SET {#query-set}

``` sql
SET param = value
```

割り当て `value` に `param` [設定](../../operations/settings/index.md) 現在のセッションの場合。 変更できません [サーバー設定](../../operations/server-configuration-parameters/index.md) こっちだ

指定した設定プロファイルのすべての値を単一のクエリで設定することもできます。

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

詳細については、 [設定](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

現在のユーザーのロールを有効にします。

### 構文 {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

既定のロールをユーザーに設定します。

デフォルトの役割を自動的に起動されたユーザーログインします。 既定として設定できるのは、以前に付与されたロールのみです。 ロールがユーザーに付与されていない場合、ClickHouseは例外をスローします。

### 構文 {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

### 例 {#set-default-role-examples}

複数の既定のロールをユーザーに設定する:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

付与されたすべてのロールを既定のユーザーに設定します:

``` sql
SET DEFAULT ROLE ALL TO user
```

ユーザーからの既定の役割の削除:

``` sql
SET DEFAULT ROLE NONE TO user
```

セットの付与の役割としてデフォルトの例外を除き、きっ:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

表からすべてのデータを削除します。 とき句 `IF EXISTS` テーブルが存在しない場合、クエリはエラーを返します。

その `TRUNCATE` クエリはサポートされません [表示](../../engines/table-engines/special/view.md), [ファイル](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) と [Null](../../engines/table-engines/special/null.md) テーブルエンジン。

## USE {#use}

``` sql
USE db
```

セッションの現在のデータベースを設定できます。
現在のデータベースは、データベースがクエリで明示的に定義されていない場合、テーブルの検索に使用されます。
セッションの概念がないため、HTTPプロトコルを使用する場合は、このクエリを実行できません。

[元の記事](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
