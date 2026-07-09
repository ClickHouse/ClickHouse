---
description: 'INSERT INTO ステートメントの説明'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'INSERT INTO ステートメント'
doc_type: 'reference'
---

データをテーブルに挿入します。

**構文**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

`(c1, c2, c3)` を使って、挿入するカラムの一覧を指定できます。また、`*` のようなカラム[マッチャー](../../sql-reference/statements/select/index.md#asterisk)を含む式や、[APPLY](/ja/sql-reference/statements/select/apply-modifier)、[EXCEPT](/ja/sql-reference/statements/select/except-modifier)、[REPLACE](/ja/sql-reference/statements/select/replace-modifier) などの[修飾子](../../sql-reference/statements/select/index.md#select-modifiers)も使用できます。

たとえば、次のテーブルを考えます。

```sql
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

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

カラム `b` を除くすべてのカラムにデータを挿入するには、`EXCEPT` キーワードを使用できます。上記の構文のとおり、指定するカラム (`(c1, c3)`) の数と挿入する値 (`VALUES (v11, v13)`) の数が一致している必要があります。

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

この例では、2 番目に挿入された行では、`a` と `c` のカラムは渡された値で埋められ、`b` にはデフォルトで値が設定されることがわかります。デフォルト値を挿入するために `DEFAULT` キーワードを使用することもできます:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

カラムの一覧に既存のカラムがすべて含まれていない場合、残りのカラムには次の値が設定されます。

* テーブル定義で指定された `DEFAULT` 式から計算された値。
* `DEFAULT` 式が定義されていない場合は、ゼロと空文字列。

データは、ClickHouse がサポートする任意の[フォーマット](/ja/sql-reference/formats)で INSERT に渡せます。フォーマットはクエリ内で明示的に指定する必要があります。

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

たとえば、次のクエリのフォーマットは `INSERT ... VALUES` の基本形と同じです:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse は、データの前にあるすべてのスペースと 1 つの改行文字 (ある場合) を削除します。クエリを作成する際は、特にデータがスペースで始まる場合に備えて、クエリの演算子の後の新しい行にデータを配置することを推奨します。

例:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

[コマンドラインクライアント](/ja/operations/utilities/clickhouse-local) または [HTTPインターフェイス](/ja/interfaces/http) を使用すると、クエリとは別にデータを挿入できます。

:::note
`INSERT` クエリに `SETTINGS` を指定する場合は、`FORMAT` 句の *前* に指定する必要があります。`FORMAT format_name` より後ろはすべてデータとして扱われるためです。例:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## 制約
</div>

テーブルに[制約](../../sql-reference/statements/create/table.md#constraints)がある場合、挿入されるデータの各行に対してその式が検証されます。いずれかの制約が満たされていない場合、サーバーは制約名と式を含む例外を返し、クエリは停止されます。

<div id="data-type-validation">
  ## データ型の検証
</div>

ClickHouse は、許可されたデータ型 (`enable_time_time64_type`、`allow_suspicious_low_cardinality_types`、`allow_suspicious_fixed_string_types` などの設定で制御) を、`INSERT` 時ではなく、テーブルの作成時 (`CREATE TABLE`) およびスキーマの変更時 (`ALTER TABLE`) にのみ検証します。

つまり、許可されていないデータ型を持つテーブルがすでに存在する場合、サーバー上で対応する設定が無効になっていても、そのテーブルにはデータを挿入できます。これは意図された動作です。いったんテーブルが作成されると、型の作成を制御する設定によって挿入が妨げられるべきではありません。

たとえば:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
その結果、ターゲットテーブルに対応するカラム型がすでに存在していれば、新しいバージョンのクライアント (設定がデフォルトで有効) から、古いバージョンのサーバー (設定が無効) に対して、許可されていないデータ型を含むデータを挿入できます。検証は DML レベルではなく、DDL レベルで行われます。
:::

<div id="inserting-the-results-of-select">
  ## SELECT の結果を挿入する
</div>

**構文**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

カラムは `SELECT` 句内での位置に従って対応付けられます。ただし、`SELECT` 式内の名前と、`INSERT` 先のテーブル内の名前は異なる場合があります。必要に応じて、型変換が行われます。

Values format を除くどのフォーマットでも、`now()`、`1 + 2` などの式に値を設定することはできません。Values format では式を限定的に使用できますが、この場合、それらの実行には非効率なコードが使われるため、推奨されません。

data parts を変更するその他のクエリはサポートされていません: `UPDATE`、`DELETE`、`REPLACE`、`MERGE`、`UPSERT`、`INSERT UPDATE`。
ただし、`ALTER TABLE ... DROP PARTITION` を使用して古いデータを削除することはできます。

`SELECT` 句に テーブル関数 [input()](../../sql-reference/table-functions/input.md) が含まれている場合、`FORMAT` 句 はクエリの末尾で指定する必要があります。

非 Nullable のデータ型を持つカラムに `NULL` の代わりにデフォルト値を挿入するには、[insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default) setting を有効にします。

`INSERT` は CTE (共通テーブル式) もサポートしています。たとえば、次の 2 つのステートメントは同等です:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## ファイルからデータを挿入する
</div>

**構文**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

上記の構文を使用すると、**クライアント**側に保存されたファイル、または複数のファイルからデータを挿入できます。`file_name` と `type` は文字列リテラルです。入力ファイルの[フォーマット](../../interfaces/formats.md)は、`FORMAT` 句で指定する必要があります。

圧縮ファイルもサポートされています。圧縮タイプはファイル名の拡張子から自動検出されます。また、`COMPRESSION` 句で明示的に指定することもできます。サポートされているタイプは次のとおりです: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`。

この機能は、[コマンドラインクライアント](../../interfaces/client.md) と [clickhouse-local](../../operations/utilities/clickhouse-local.md) で利用できます。

**例**

<div id="single-file-with-from-infile">
  ### FROM INFILE を使用した単一ファイル
</div>

[コマンドラインクライアント](../../interfaces/client.md) を使用して、次のクエリを実行します。

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### glob を使用した FROM INFILE による複数ファイルの読み込み
</div>

この例は前のものと非常によく似ていますが、`FROM INFILE 'input_*.csv'` を使って複数のファイルから挿入を行います。

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
`*` を使って複数のファイルを選択するだけでなく、範囲指定 (`{1,2}` または `{1..9}`) やその他の[glob 置換](/ja/sql-reference/table-functions/file.md/#globs-in-path)も利用できます。以下の 3 つはいずれも、上記の例で使用できます。

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## テーブル関数を使用した挿入
</div>

[テーブル関数](../../sql-reference/table-functions/index.md)で参照されるテーブルにデータを挿入できます。

**構文**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**例**

次のクエリでは、[remote](/ja/sql-reference/table-functions/remote) テーブル関数を使用します。

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## ClickHouse Cloud への挿入
</div>

デフォルトでは、ClickHouse Cloud のサービスは高可用性のために複数のレプリカを提供します。サービスに接続すると、それらのレプリカのいずれかへの接続が確立されます。

`INSERT` が成功すると、データは基盤となるストレージに書き込まれます。ただし、レプリカがこれらの更新を受け取るまでに少し時間がかかる場合があります。そのため、別の接続を使用して他のレプリカのいずれかで `SELECT` クエリを実行すると、更新されたデータがまだ反映されていない可能性があります。

`select_sequential_consistency` を使用すると、レプリカが最新の更新を確実に受け取るようにできます。以下は、この設定を使用した `SELECT` クエリの例です。

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

`select_sequential_consistency` を使用すると、ClickHouse Keeper (ClickHouse Cloud で内部的に使用されています) への負荷が増加し、サービスの負荷状況によってはパフォーマンスが低下する可能性がある点に注意してください。必要な場合を除き、この設定を有効にすることは推奨しません。推奨される方法は、同じセッションで読み取り/書き込みを実行するか、ネイティブプロトコルを使用するクライアントドライバー (つまり、スティッキー接続をサポートするもの) を使用することです。

<div id="inserting-into-a-replicated-setup">
  ## レプリケート構成への挿入
</div>

レプリケート構成では、データはレプリケーションが完了してから他のレプリカで参照できるようになります。データのレプリケーション (他のレプリカへのダウンロード) は、`INSERT` の直後に開始されます。これは ClickHouse Cloud とは異なり、ClickHouse Cloud ではデータは即座に共有ストレージへ書き込まれ、レプリカはメタデータの変更を購読します。

レプリケート構成では、分散コンセンサスのために ClickHouse Keeper へのコミットが必要になるため、`INSERTs` にかなり時間 (1 秒程度) がかかることがある点に注意してください。また、ストレージに S3 を使用すると、さらにレイテンシが増加します。

<div id="performance-considerations">
  ## パフォーマンスに関する考慮事項
</div>

`INSERT` は入力データを主キーでソートし、パーティションキーでパーティションに分割します。一度に複数のパーティションへデータを挿入すると、`INSERT` クエリのパフォーマンスが大幅に低下する可能性があります。これを避けるには、次のようにします。

* 一度に 100,000 行程度の、十分に大きなバッチでデータを追加します。
* ClickHouse にアップロードする前に、パーティションキーごとにデータをグループ化します。

次の場合は、パフォーマンスは低下しません。

* データがリアルタイムで追加される場合。
* 通常は時刻でソートされているデータをアップロードする場合。

<div id="asynchronous-inserts">
  ### 非同期挿入
</div>

小規模ながら高頻度の挿入では、データを非同期で挿入できます。このような挿入のデータはバッチにまとめられ、その後安全にテーブルへ挿入されます。非同期挿入を使用するには、[`async_insert`](/ja/operations/settings/settings#async_insert) 設定を有効にします。

`async_insert` または [`Buffer` テーブルエンジン](/ja/engines/table-engines/special/buffer) を使用すると、追加のバッファリングが発生します。

<div id="large-or-long-running-inserts">
  ### 大規模または長時間実行されるインサート
</div>

大量のデータを挿入する場合、ClickHouse は「まとめる」と呼ばれる処理によって書き込み性能を最適化します。メモリ内にある挿入データの小さなブロックは、ディスクに書き込まれる前にマージされ、より大きなブロックにまとめられます。これにより、各書き込み操作に伴うオーバーヘッドが軽減されます。この処理では、ClickHouse が [`max_insert_block_size`](/ja/operations/settings/settings#max_insert_block_size) 行ごとの書き込みを完了するたびに、その時点までに挿入されたデータをクエリできるようになります。

**関連項目**

* [async&#95;insert](/ja/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/ja/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/ja/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/ja/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/ja/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/ja/operations/settings/settings#async_insert_max_data_size)