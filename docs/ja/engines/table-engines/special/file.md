---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u30D5\u30A1\u30A4\u30EB"
---

# ファイル {#table_engines-file}

ファイルにテーブルエンジンのデータをファイルを使ったり、 [ファイル
形式](../../../interfaces/formats.md#formats) (TabSeparated、Nativeなど).

使用例:

-   ClickHouseからファイルにデータエクスポート。
-   ある形式から別の形式にデータを変換します。
-   データ更新にClickHouse経由で編集ファイルディスク。

## ClickHouseサーバーでの使用状況 {#usage-in-clickhouse-server}

``` sql
File(Format)
```

その `Format` パラメータを指定するか、ファイルのファイルフォーマット 実行するには
`SELECT` この形式は、入力と実行のためにサポートされている必要があります
`INSERT` queries – for output. The available formats are listed in the
[形式](../../../interfaces/formats.md#formats) セクション

ClickHouseはファイルシステムのパスを`File`. で定義されたフォルダを使用します [パス](../../../operations/server-configuration-parameters/settings.md) サーバー構成の設定。

を使用して表を作成する場合 `File(Format)` で空のサブディレクトリとフォルダにまとめた。 データがそのテーブルに書き込まれると、 `data.Format` そのサブディレ

お手動で作成このサブフォルダやファイルサーバのファイルシステムとし [ATTACH](../../../sql-reference/statements/misc.md) 一致する名前で情報を表すので、そのファイルからデータを照会することができます。

!!! warning "警告"
    ClickHouseはそのようなファイルへの外部の変更を追跡しないので、この機能に注意してください。 ClickHouseとClickHouseの外部での同時書き込みの結果は未定義です。

**例:**

**1.** セットアップ `file_engine_table` テーブル:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

デフォルトでClickHouseフォルダを作成します `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** 手動で作成 `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` 含む:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** データの照会:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## ClickHouseでの使用-ローカル {#usage-in-clickhouse-local}

で [ﾂつｨﾂ姪"ﾂ債ﾂつｹ](../../../operations/utilities/clickhouse-local.md) ファイルエンジンは、以下に加えて `Format`. デフォルトの入出力ストリームは、次のような数値または人間が読める名前で指定できます `0` または `stdin`, `1` または `stdout`.
**例:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## 実施内容 {#details-of-implementation}

-   複数 `SELECT` クエリは同時に実行できますが `INSERT` クエリは互いに待機します。
-   新しいファイルを作成する `INSERT` クエリ。
-   ファイルが存在する場合, `INSERT` それに新しい値を追加します。
-   対応していません:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   指数
    -   複製

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
