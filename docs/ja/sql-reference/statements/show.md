---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 38
toc_title: SHOW
---

# クエリを表示 {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

シングルを返します `String`-タイプ ‘statement’ column, which contains a single value – the `CREATE` 指定したオブジェク

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

一覧の全てのデータベースです。
このクエリは次と同じです `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

の内容を出力します [システム。プロセス](../../operations/system-tables.md#system_tables-processes) 現在処理中のクエリのリストを含むテーブル。 `SHOW PROCESSLIST` クエリ。

その `SELECT * FROM system.processes` クエリを返しますデータに現在のすべてのクエリ.

ヒント（コンソールで実行):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

テーブルのリストを表示します。

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

この `FROM` 句が指定されていない場合、クエリは現在のデータベースからテーブルの一覧を返します。

あなたは同じ結果を得ることができます `SHOW TABLES` 次の方法でクエリ:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例えば**

次のクエリは、テーブルのリストから最初の二つの行を選択します。 `system` 名前に含まれるデータベース `co`.

``` sql
SHOW TABLES FROM system LIKE '%co%' LIMIT 2
```

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ collations                     │
└────────────────────────────────┘
```

## SHOW DICTIONARIES {#show-dictionaries}

リストをの表示します [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

この `FROM` 句が指定されていない場合、クエリは現在のデータベースから辞書のリストを返します。

あなたは同じ結果を得ることができます `SHOW DICTIONARIES` 次の方法でクエリ:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例えば**

次のクエリは、テーブルのリストから最初の二つの行を選択します。 `system` 名前に含まれるデータベース `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
