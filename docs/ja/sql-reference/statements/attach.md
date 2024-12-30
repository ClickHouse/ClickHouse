---
slug: /ja/sql-reference/statements/attach
sidebar_position: 40
sidebar_label: ATTACH
title: "ATTACH ステートメント"
---

データベースを別のサーバーに移動する場合などに、テーブルやDictionaryをアタッチします。

**構文**

``` sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

クエリはディスク上にデータを作成せず、データが適切な場所に既にあると仮定して、指定したテーブル、Dictionary、またはデータベースに関する情報をサーバーに追加します。`ATTACH`クエリを実行した後、サーバーはそのテーブル、Dictionary、またはデータベースの存在を認識します。

テーブルが以前に切り離されていた場合（[DETACH](../../sql-reference/statements/detach.md)クエリ）、その構造が既知であることを意味するので、構造を定義せずに省略形を使用できます。

## 既存のテーブルをアタッチ

**構文**

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

このクエリはサーバーを開始する際に使用されます。サーバーはテーブルメタデータを`ATTACH`クエリとしてファイルとして保存し、起動時に単に実行します（サーバー上で明示的に作成される一部のシステムテーブルを除く）。

テーブルが永久に切り離されていた場合、サーバーの起動時には再アタッチされないため、`ATTACH`クエリを明示的に使用する必要があります。

## 新しいテーブルを作成しデータをアタッチ

### テーブルデータへの指定されたパスを使用する場合

このクエリは、提供された構造で新しいテーブルを作成し、`user_files`にある指定されたディレクトリからテーブルデータをアタッチします。

**構文**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**例**

クエリ:

```sql
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```
結果:

```sql
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

### 指定されたテーブルUUIDを使用する場合

このクエリは、提供された構造で新しいテーブルを作成し、指定されたUUIDのテーブルからデータをアタッチします。これは[Atomic](../../engines/database-engines/atomic.md)データベースエンジンによってサポートされています。

**構文**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

## 既存のDictionaryをアタッチ

以前に切り離されたDictionaryをアタッチします。

**構文**

``` sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

## 既存のデータベースをアタッチ

以前に切り離されたデータベースをアタッチします。

**構文**

``` sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```
