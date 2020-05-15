---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u30D5\u30A1\u30A4\u30EB"
---

# ファイル {#table_engines-file}

ファイルにテーブルエンジンのデータをファイルを使ったり、 [ファイル
形式](../../../interfaces/formats.md#formats) （TabSeparated、Nativeなど）。).

使用例:

-   データからの輸出clickhouseるファイルです。
-   ある形式から別の形式にデータを変換します。
-   ディスク上のファイルを編集して、clickhouseのデータを更新する。

## Clickhouseサーバーでの使用状況 {#usage-in-clickhouse-server}

``` sql
File(Format)
```

その `Format` パラメータを指定するか、ファイルのファイルフォーマット 実行するには
`SELECT` クエリは、形式は、入力のためにサポートされ、実行する必要があります
`INSERT` queries – for output. The available formats are listed in the
[形式](../../../interfaces/formats.md#formats) セクション。

クリックハウ`File`. で定義されたフォルダを使用します [パス](../../../operations/server-configuration-parameters/settings.md) サーバー構成での設定。

テーブルを作成するとき `File(Format)` で空のサブディレクトリとフォルダにまとめた。 データがそのテーブルに書き込まれると、 `data.Format` サブディレクト

このサブフォルダとファイルをserver filesystemに手動で作成してから [ATTACH](../../../sql-reference/statements/misc.md) でテーブルの情報をマッチングの名前でデータベースバックエンドからファイルです。

!!! warning "警告"
    ClickHouseはそのようなファイルの外部変更を追跡しないため、この機能には注意してください。 ClickHouseを介して同時に書き込みを行い、ClickHouseの外部に書き込みを行った結果は未定義です。

**例えば:**

**1.** セットアップ `file_engine_table` テーブル:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

デフォルトでclickhouseフォルダを作成します `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** 手動で作成する `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` を含む:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** データのクエリ:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Clickhouseでの使用-ローカル {#usage-in-clickhouse-local}

で [ﾂつ"ﾂづ按つｵﾂ！](../../../operations/utilities/clickhouse-local.md) ファイルエンジ `Format`. デフォルトの入力/出力ストリームは、数値または人間が読める名前を使用して指定できます `0` または `stdin`, `1` または `stdout`.
**例えば:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## 実装の詳細 {#details-of-implementation}

-   複数 `SELECT` クエリは同時に実行できますが、 `INSERT` クエリはお互いを待ちます。
-   新しいファイルの作成に対応 `INSERT` クエリ。
-   ファイルが存在する場合, `INSERT` それに新しい値を追加します。
-   サポートなし:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   指数
    -   複製

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
