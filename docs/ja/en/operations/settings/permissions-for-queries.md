---
description: 'クエリ権限の設定。'
sidebar_label: 'クエリ権限'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: 'クエリ権限'
doc_type: 'reference'
---

ClickHouse のクエリは、いくつかの種類に分類できます。

1. データの読み取りクエリ: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. データの書き込みクエリ: `INSERT`, `OPTIMIZE`.
3. 設定変更クエリ: `SET`, `USE`.
4. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

以下の設定は、クエリの種類ごとにユーザー権限を制御します:

<div id="readonly">
  ## readonly
</div>

データの読み取り、データの書き込み、および設定変更クエリの権限を制限します。

1 に設定すると、次が許可されます。

* すべての種類の読み取りクエリ (SELECT およびそれに相当するクエリなど) 。
* セッションコンテキストのみを変更するクエリ (USE など) 。

2 に設定すると、上記に加えて次も許可されます。

* SET および CREATE TEMPORARY TABLE

  :::tip
  EXISTS、DESCRIBE、EXPLAIN、SHOW PROCESSLIST などのクエリは、システムテーブルに対して SELECT するだけなので、SELECT と同等です。
  :::

設定可能な値:

* 0 — 閲覧、書き込み、および設定変更クエリが許可されます。
* 1 — データの読み取りクエリのみが許可されます。
* 2 — データの読み取りクエリと設定変更クエリが許可されます。

デフォルト値: 0

:::note
`readonly = 1` を設定すると、現在のセッションではユーザーは `readonly` および `allow_ddl` の設定を変更できません。

[HTTPインターフェイス](/ja/interfaces/http) で `GET` メソッドを使用すると、`readonly = 1` が自動的に設定されます。データを変更するには、`POST` メソッドを使用してください。

`readonly = 1` を設定すると、ユーザーは設定を変更できなくなります。特定の設定だけ変更を禁止する方法もあります。また、`readonly = 1` の制限下で特定の設定だけ変更を許可する方法もあります。詳細は[設定に対する制約](../../operations/settings/constraints-on-settings.md)を参照してください。
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

[DDL](https://en.wikipedia.org/wiki/Data_definition_language)クエリを許可または禁止します。

設定可能な値:

* 0 — DDLクエリは許可されません。
* 1 — DDLクエリは許可されます。

デフォルト値: 1

:::note
現在のセッションで `allow_ddl = 0` の場合、`SET allow_ddl = 1` は実行できません。
:::

:::note KILL QUERY
`KILL QUERY` は、readonly と allow&#95;ddl の設定がどのような組み合わせでも実行できます。
:::