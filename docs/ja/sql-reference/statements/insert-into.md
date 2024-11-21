# INSERT INTO ステートメント

テーブルにデータを挿入します。

**構文**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

挿入するカラムの一覧を `(c1, c2, c3)` で指定できます。また、`*` や [APPLY](../../sql-reference/statements/select/index.md#apply-modifier), [EXCEPT](../../sql-reference/statements/select/index.md#except-modifier), [REPLACE](../../sql-reference/statements/select/index.md#replace-modifier) などの[修飾子](../../sql-reference/statements/select/index.md#select-modifiers)を使用したカラム[マッチャー](../../sql-reference/statements/select/index.md#asterisk)の式も使用可能です。

例えば、以下のテーブルを考えます:

``` sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

``` sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

すべてのカラムにデータを挿入し、'b' カラムを除外したい場合は、丸括弧内に選んだカラムの数だけ値を渡す必要があります:

``` sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

``` sql
SELECT * FROM insert_select_testtable;
```

```
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

この例では、2行目で挿入された行は、`a` と `c` カラムが渡された値で埋められ、`b` カラムはデフォルト値で埋められています。`DEFAULT` キーワードを使用してデフォルト値を挿入することも可能です:

``` sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

カラムのリストにすべての既存カラムが含まれていない場合、残りのカラムは以下で埋められます:

- テーブル定義で指定されている `DEFAULT` 式から計算された値。
- `DEFAULT` 式が定義されていない場合は、ゼロと空文字列。

データは ClickHouse がサポートする任意の[フォーマット](../../interfaces/formats.md#formats)で `INSERT` に渡すことができます。クエリでフォーマットを明示的に指定する必要があります:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

例えば、次のクエリフォーマットは基本的な INSERT ... VALUES と同じです:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse はデータの前のすべてのスペースと1行の改行（存在する場合）を削除します。クエリを形成する際、データをクエリオペレータの後に新しい行に置くことをお勧めします（データがスペースで始まる場合は重要です）。

例:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

データをクエリから分離して挿入するために、[コマンドラインクライアント](/docs/ja/integrations/sql-clients/clickhouse-client-local) や [HTTPインターフェース](/docs/ja/interfaces/http/)を使用できます。

:::note
`INSERT` クエリに `SETTINGS` を指定したい場合は、`FORMAT` 節の _前に_ 設定する必要があります。なぜなら、`FORMAT format_name` 以降はすべてデータと見なされるからです。例:
```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```
:::

## 制約

テーブルに[制約](../../sql-reference/statements/create/table.md#constraints)がある場合、その式は挿入データの各行に対してチェックされます。それらの制約のいずれかが満たされていない場合、サーバーは制約名と式を含む例外を発生させ、クエリは中断されます。

## SELECT の結果の挿入

**構文**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

カラムは `SELECT` 節での位置に従ってマッピングされます。しかし、`SELECT` 式と `INSERT` のためのテーブルでの名前は異なる場合があります。必要に応じて型キャスティングが行われます。

`Values` 以外のデータフォーマットは、`now()`, `1 + 2` などのような式に値を設定することを許可しません。`Values` フォーマットは式の限定的な使用を許可しますが、これは推奨されません。なぜなら、この場合、非効率的なコードが使用されるからです。

他のデータパーツを修正するクエリはサポートされていません：`UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`。ただし、`ALTER TABLE ... DROP PARTITION` を使用して古いデータを削除できます。

`SELECT` 節がテーブル関数 [input()](../../sql-reference/table-functions/input.md) を含む場合は、`FORMAT` 節をクエリの最後に指定する必要があります。

`NULL` を持つ非nullableなデータタイプのカラムに代わりにデフォルト値を挿入するには、[insert_null_as_default](../../operations/settings/settings.md#insert_null_as_default) 設定を有効にします。

`INSERT` はまた、CTE（共通テーブル式）をサポートしています。例えば、以下の2つの文は同等です：

``` sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

## ファイルからデータを挿入

**構文**

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

上記の構文を使用して、**クライアント**側に保存されているファイル、またはファイル群からデータを挿入します。`file_name` と `type` は文字列リテラルです。入力ファイルの[フォーマット](../../interfaces/formats.md)は `FORMAT` 節で設定する必要があります。

圧縮ファイルはサポートされています。圧縮タイプはファイル名の拡張子で検出されます。もしくは、`COMPRESSION` 節で明示的に指定できます。サポートされているタイプは： `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`。

この機能は[コマンドラインクライアント](../../interfaces/cli.md)および[clickhouse-local](../../operations/utilities/clickhouse-local.md)で利用可能です。

**例**

### FROM INFILE を使用した単一ファイル
以下のクエリを[コマンドラインクライアント](../../interfaces/cli.md)で実行します：

```bash
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

結果:

```text
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

### FROM INFILE を使用した複数ファイルのグロブ

この例は前のものと非常に似ていますが、`FROM INFILE 'input_*.csv` を使用して複数のファイルから挿入します。

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
複数のファイルを `*` で選択することに加え、範囲（`{1,2}` や `{1..9}`）やその他の[グロブ置換](/docs/ja/sql-reference/table-functions/file.md/#globs-in-path)を使用できます。以下の3つすべてが上記の例で機能します：
```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```
:::

## テーブル関数を使ったデータの挿入

データは[テーブル関数](../../sql-reference/table-functions/index.md)で参照されるテーブルに挿入できます。

**構文**
``` sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**例**

次のクエリでは、[remote](../../sql-reference/table-functions/index.md#remote) テーブル関数を使用しています:

``` sql
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

結果:

``` text
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

## ClickHouse Cloud へのデータ挿入

デフォルトでは、ClickHouse Cloud のサービスは高可用性のために複数のレプリカを提供します。サービスに接続すると、これらのレプリカのいずれかに接続が確立されます。

`INSERT` が成功すると、データは基盤となるストレージに書き込まれます。しかし、レプリカがこれらの更新を受け取るまでには時間がかかる場合があります。そのため、別の接続を使用してこれらの他のレプリカのいずれかで `SELECT` クエリを実行した場合、更新されたデータがまだ反映されていない可能性があります。

レプリカに最新の更新を受け取らせるためには、`select_sequential_consistency` を使用することが可能です。ここにその設定を使用した SELECT クエリの例を示します：

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

`select_sequential_consistency` を使用すると、ClickHouse Keeper に負荷がかかる可能性があり、サービスの負荷に応じてパフォーマンスが低下する可能性があることに注意してください。必要でない限り、この設定を有効にすることはお勧めしません。推奨されるアプローチは、同じセッションで読み取り/書き込みを実行するか、ネイティブプロトコルを使用するクライアントドライバを使用することです（これによりスティッキー接続がサポートされます）。

## レプリケーション設定での挿入

レプリケーション設定では、データはレプリケーションされた後に他のレプリカで表示されます。データは `INSERT` 後すぐにレプリケーション（他のレプリカでダウンロード）を開始します。これは、データがすぐに共有ストレージに書き込まれ、レプリカがメタデータの変更を購読する ClickHouse Cloud とは異なります。

レプリケーション設定では、`INSERTs` が時には（1秒程度の）かなりの時間を要する可能性があることに注意してください。これは、分散コンセンサスのために ClickHouse Keeper にコミットする必要があるためです。S3 をストレージに使用することも、追加のレイテンシーを追加します。

## パフォーマンスに関する考慮事項

`INSERT` は入力データを主キーでソートし、パーティションキーでパーティションに分割します。一度に複数のパーティションにデータを挿入すると、`INSERT` クエリのパフォーマンスが大幅に低下する可能性があります。これを避けるために：

- 例えば 100,000 行のように、かなり大きなバッチでデータを追加する。
- データをアップロードする前にパーティションキーでグループ化する。

以下の場合、パフォーマンスは低下しません：

- データがリアルタイムで追加される。
- 通常、時間でソートされたデータをアップロードする。

### 非同期挿入

小さくても頻繁に非同期でデータを挿入することが可能です。そのような挿入からのデータはバッチに結合され、安全にテーブルに挿入されます。非同期挿入を使用するには、[`async_insert`](../../operations/settings/settings.md#async-insert) 設定を有効にします。

`async_insert` または [`Buffer` テーブルエンジン](/ja/engines/table-engines/special/buffer) を使用することで、追加のバッファリングが発生します。

### 大規模または長時間実行の挿入

大量のデータを挿入するとき、ClickHouse は「スクワッシュ」と呼ばれるプロセスを通じて書き込みパフォーマンスを最適化します。メモリ内の小さなブロックの挿入データが結合され、ディスクに書き込まれる前に大きなブロックにスクワッシュされます。スクワッシュは、各書き込み操作に関連するオーバーヘッドを削減します。このプロセスでは、ClickHouse が各[`max_insert_block_size`](/ja/operations/settings/settings#max_insert_block_size) 行を書き込むのを完了した後、挿入されたデータがクエリ可能となります。

**関連項目**

- [async_insert](../../operations/settings/settings.md#async-insert)
- [async_insert_threads](../../operations/settings/settings.md#async-insert-threads)
- [wait_for_async_insert](../../operations/settings/settings.md#wait-for-async-insert)
- [wait_for_async_insert_timeout](../../operations/settings/settings.md#wait-for-async-insert-timeout)
- [async_insert_max_data_size](../../operations/settings/settings.md#async-insert-max-data-size)
- [async_insert_busy_timeout_ms](../../operations/settings/settings.md#async-insert-busy-timeout-ms)
- [async_insert_stale_timeout_ms](../../operations/settings/settings.md#async-insert-stale-timeout-ms)
