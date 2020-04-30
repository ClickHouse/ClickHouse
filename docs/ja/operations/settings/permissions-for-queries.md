---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 58
toc_title: "\u30AF\u30A8\u30EA\u306E\u6A29\u9650"
---

# クエリの権限 {#permissions_for_queries}

問合せclickhouse大きく分けて複数の種類:

1.  データを読み込むためのクエリー: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  データクエリの記述: `INSERT`, `OPTIMIZE`.
3.  設定の変更クエリ: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

次の設定では、クエリの種類に応じてユーザー権限を調整します:

-   [読み取り専用](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow\_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` 任意の設定で実行できます。

## 読み取り専用 {#settings_readonly}

データの読み取り、データの書き込み、設定の変更の権限を制限します。

クエリを型に分割する方法を参照してください [上](#permissions_for_queries).

可能な値:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

設定後 `readonly = 1`、ユーザーは変更できません `readonly` と `allow_ddl` 現在のセッションの設定。

を使用する場合 `GET` の方法 [HTTPインター](../../interfaces/http.md), `readonly = 1` 自動的に設定されます。 データを変更するには、 `POST` 方法。

設定 `readonly = 1` ユーザーによるすべての設定の変更を禁止します。 ユーザーを禁止する方法があります
特定の設定のみを変更するから、詳細については [設定の制約](constraints-on-settings.md).

デフォルト値:0

## allow\_ddl {#settings_allow_ddl}

許可または拒否 [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ。

クエリを型に分割する方法を参照してください [上](#permissions_for_queries).

可能な値:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

あなたは実行できません `SET allow_ddl = 1` もし `allow_ddl = 0` 現在のセッションの場合。

デフォルト値:1

[元の記事](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
