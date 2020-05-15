---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u305D\u306E\u4ED6"
---

# その他のクエリ {#miscellaneous-queries}

## ATTACH {#attach}

このクエリは `CREATE`、しかし

-   単語の代わりに `CREATE` それは単語を使用します `ATTACH`.
-   クエリはディスク上にデータを作成しませんが、データがすでに適切な場所にあるとみなし、テーブルに関する情報をサーバーに追加するだけです。
    添付クエリを実行すると、サーバーはテーブルの存在を知ることになります。

テーブルが以前に分離されていた場合 (`DETACH`）、その構造が知られていることを意味し、構造を定義することなく省略形を使用することができます。

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

このクエリは、サーバーの起動時に使用されます。 サーバーに店舗のテーブルメタデータとしてファイル `ATTACH` クエリは、起動時に実行されるだけです（サーバー上に明示的に作成されたシステムテーブルは例外です）。

## CHECK TABLE {#check-table}

Trueの場合、ただちにパージを行うデータをテーブルが壊れる可能性があります。

``` sql
CHECK TABLE [db.]name
```

その `CHECK TABLE` queryは、実際のファイルサイズをサーバーに格納されている期待値と比較します。 ファイルサイズが格納された値と一致しない場合は、データが破損していることを意味します。 このが発生する可能性があります、例えば、システムがクラッシュ時のクエリを実行します。

のクエリーの応答を含む `result` 単一行の列。 行に値がのあります
[ブール値](../../sql-reference/data-types/boolean.md) タイプ:

-   0-テーブル内のデータが破損しています。
-   1-データは整合性を維持します。

その `CHECK TABLE` クエリは以下のテーブルエンジン:

-   [ログ](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [ストリップログ](../../engines/table-engines/log-family/stripelog.md)
-   [マージツリーファミリー](../../engines/table-engines/mergetree-family/mergetree.md)

これは、テーブルが別のテーブルエンジンの原因となる例外です。

からのエンジン `*Log` 家族は失敗の自動データ回復を提供しない。 を使用 `CHECK TABLE` タイムリーにデータ損失を追跡するためのクエリ。

のために `MergeTree` 家族のエンジンは、 `CHECK TABLE` クエリを示すステータス確認のための個人データのテーブルに現地サーバーです。

**データが破損している場合**

テーブルが破損している場合は、破損していないデータを別のテーブルにコピーできます。 これを行うには:

1.  破損したテーブルと同じ構造の新しいテーブルを作成します。 このためクセスしてください `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  セットを [max\_threads](../../operations/settings/settings.md#settings-max_threads) 単一のスレッドで次のクエリを処理するには、1の値を指定します。 これを行うにはクエリを実行します `SET max_threads = 1`.
3.  クエリの実行 `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. この要求にコピーする非破損データからの表-別表に示す。 破損した部分の前のデータのみがコピーされます。
4.  再起動する `clickhouse-client` リセットするには `max_threads` 値。

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

以下を返します `String` タイプ列:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [既定の式](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` または `ALIAS`). 既定の式が指定されていない場合、Columnには空の文字列が含まれます。
-   `default_expression` — Value specified in the `DEFAULT` 句。
-   `comment_expression` — Comment text.

入れ子にされたデータ構造は “expanded” フォーマット。 各列は別々に表示され、名前はドットの後に続きます。

## DETACH {#detach}

に関する情報を削除します。 ‘name’ サーバーからのテーブル。 サーバーはテーブルの存在を知ることをやめます。

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

テーブルのデータやメタデータは削除されません。 次のサーバーの起動時に、サーバーはメタデータを読み取り、再度テーブルについて調べます。
同様に、 “detached” 表はを使用して再付けることができます `ATTACH` クエリ(メタデータが格納されていないシステムテーブルを除く)。

ありません `DETACH DATABASE` クエリ。

## DROP {#drop}

このクエ: `DROP DATABASE` と `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

内部のすべてのテーブルを削除 ‘db’ データベース、その後削除 ‘db’ データベース自体。
もし `IF EXISTS` データベースが存在しない場合、エラーは返されません。

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

テーブルを削除します。
もし `IF EXISTS` テーブルが存在しない場合、またはデータベースが存在しない場合は、エラーを返しません。

    DROP DICTIONARY [IF EXISTS] [db.]name

辞書をdeletsします。
もし `IF EXISTS` テーブルが存在しない場合、またはデータベースが存在しない場合は、エラーを返しません。

## EXISTS {#exists}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

シングルを返します `UInt8`-単一の値を含むタイプの列 `0` テーブルまたはデータベースが存在しない場合、または `1` 指定されたデータベースにテーブルが存在する場合。

## KILL QUERY {#kill-query}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

現在実行中のクエリを強制的に終了しようとします。
終了するクエリは、システムから選択されます。プロセステーブルで定義された基準を使用して、 `WHERE` の句 `KILL` クエリ。

例:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

読み取り専用ユーザーは、自分のクエリのみを停止できます。

既定では、非同期バージョンのクエリが使用されます (`ASYNC`）、クエリが停止したことの確認を待つことはありません。

同期バージョン (`SYNC`)待機のためのすべての問い合わせに対応停止に関する情報を表示し各工程で停止します。
応答には、 `kill_status` 次の値を取ることができる列:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can’t be stopped.

テストクエリ (`TEST`）ユーザーの権限のみをチェックし、停止するクエリのリストを表示します。

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

キャンセルと削除を試みます [突然変異](alter.md#alter-mutations) これは現在実行中です。 取り消すべき突然変異はから選ばれます [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) テーブルのフィルタで指定された `WHERE` の句 `KILL` クエリ。

テストクエリ (`TEST`）ユーザーの権限のみをチェックし、停止するクエリのリストを表示します。

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

このクエリを初期化予定外の統合データのパーツを使ったテーブルのテーブルエンジンからの [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族

その `OPTMIZE` クエリもサポートされています [MaterializedView](../../engines/table-engines/special/materializedview.md) そして [バッファ](../../engines/table-engines/special/buffer.md) エンジン その他のテーブルエンジンなサポート。

とき `OPTIMIZE` とともに使用されます [レプリケートされたmergetree](../../engines/table-engines/mergetree-family/replication.md) テーブルエンジンのファミリ、ClickHouseはマージのためのタスクを作成し、すべてのノードでの実行を待ちます。 `replication_alter_partitions_sync` 設定が有効になっています)。

-   もし `OPTIMIZE` 何らかの理由でマージを実行せず、クライアントに通知しません。 通知を有効にするには、以下を使用します [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) 設定。
-   を指定した場合 `PARTITION` 指定したパーティションのみが最適化されます。 [パーティション式の設定方法](alter.md#alter-how-to-specify-part-expr).
-   指定した場合 `FINAL`、最適化は、すべてのデータがすでに一つの部分にある場合でも行われる。
-   指定した場合 `DEDUPLICATE`、その後、完全に同一の行が重複排除されます（すべての列が比較されます）、それだけでMergeTreeエンジンのために理にかなっています。

!!! warning "警告"
    `OPTIMIZE` 修正できません “Too many parts” エラー。

## RENAME {#misc_operations-rename}

テーブルの名前を変更します。

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

すべてのテーブル名変更"グローバルチェンジにおけるロックしなければなりません。 テーブルの名前を変更するのは簡単な操作です。 場合は正しく表示す他のデータベースのテーブルに表示されるようになります。本データベースです。 しかし、そのディレクトリのデータベースに格納してある必要がある同一ファイルシステム(それ以外の場合、エラーを返す。

## SET {#query-set}

``` sql
SET param = value
```

割り当て `value` に `param` [設定](../../operations/settings/index.md) 現在のセッションの場合。 変更はできません [サーバー設定](../../operations/server-configuration-parameters/index.md) こっちだ

また、指定した設定プロファイルのすべての値を単一のクエリで設定することもできます。

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

詳細については、 [設定](../../operations/settings/settings.md).

## TRUNCATE {#truncate}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

表からすべてのデータを削除します。 ときに句 `IF EXISTS` テーブルが存在しない場合、クエリはエラーを返します。

その `TRUNCATE` queryはサポートされていません [ビュー](../../engines/table-engines/special/view.md), [ファイル](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) と [ヌル](../../engines/table-engines/special/null.md) テーブルエンジン。

## USE {#use}

``` sql
USE db
```

セッションの現在のデータベースを設定できます。
現在のデータベース検索用テーブルの場合はデータベースが明示的に定義されたクエリードの前にテーブルの名前です。
このクエリできません利用の場合は、httpプロトコルが存在しない概念です。

[元の記事](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
