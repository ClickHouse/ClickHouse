---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: SHOW
---

# クエリを表示 {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

単一を返します `String`-タイプ ‘statement’ column, which contains a single value – the `CREATE` 指定したオブジェクトの作成に使用するクエリ。

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

の内容を出力します。 [システムプロセス](../../operations/system-tables.md#system_tables-processes) 現在処理されているクエリのリストを含むテーブル。 `SHOW PROCESSLIST` クエリ。

その `SELECT * FROM system.processes` クエリを返しますデータに現在のすべてのクエリ.

Tip(コンソールで実行):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

テーブルの一覧を表示します。

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

もし `FROM` 句が指定されていない場合、クエリは現在のデータベースからテーブルの一覧を返します。

と同じ結果を得ることができます `SHOW TABLES` 次の方法でクエリを実行します:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例**

次のクエリでは、テーブルのリストから最初の二つの行を選択します。 `system` 名前が含まれるデータベース `co`.

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

のリストを表示します [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

もし `FROM` 句が指定されていない場合、クエリは現在のデータベースから辞書のリストを返します。

と同じ結果を得ることができます `SHOW DICTIONARIES` 次の方法でクエリを実行します:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例**

次のクエリでは、テーブルのリストから最初の二つの行を選択します。 `system` 名前が含まれるデータベース `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW GRANTS {#show-grants-statement}

ユーザーの権限を表示します。

### 構文 {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

Userが指定されていない場合、クエリは現在のユーザーの特権を返します。

## SHOW CREATE USER {#show-create-user-statement}

Aで使用されたパラメータを示します [ユーザー作成](create.md#create-user-statement).

`SHOW CREATE USER` ユーザパスワードを出力しません。

### 構文 {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Aで使用されたパラメータを示します [ロールの作成](create.md#create-role-statement)

### 構文 {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Aで使用されたパラメータを示します [行ポリシーの作成](create.md#create-row-policy-statement)

### 構文 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Aで使用されたパラメータを示します [クォータの作成](create.md#create-quota-statement)

### 構文 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Aで使用されたパラメータを示します [設定プロファイルの作成](create.md#create-settings-profile-statement)

### 構文 {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[元の記事](https://clickhouse.com/docs/en/query_language/show/) <!--hide-->
