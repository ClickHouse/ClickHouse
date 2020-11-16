---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u30AF\u30A8\u30EA\u306E\u6A29\u9650"
---

# クエリの権限 {#permissions_for_queries}

問合せClickHouse大きく分けて複数の種類:

1.  データを読み込むためのクエリー: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  データクエリの記述: `INSERT`, `OPTIMIZE`.
3.  設定の変更クエリ: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

次の設定では、クエリの種類によってユーザー権限を調整します:

-   [読み取り専用](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` 任意の設定で実行できます。

## 読み取り専用 {#settings_readonly}

制限のアクセス権読書データの書き込みデータや設定の変更ます。

クエリを型に分割する方法を参照してください [上記](#permissions_for_queries).

可能な値:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

設定後 `readonly = 1` ユーザーは変更できません `readonly` と `allow_ddl` 現在のセッションの設定。

を使用する場合 `GET` の方法 [HTTPインターフェ](../../interfaces/http.md), `readonly = 1` 自動的に設定されます。 データを変更するには、 `POST` 方法。

設定 `readonly = 1` ユーザーがすべての設定を変更するのを禁止します。 ユーザーを禁止する方法があります
からの変更は、特定の設定の詳細を見る [設定の制約](constraints-on-settings.md).

デフォルト値:0

## allow_ddl {#settings_allow_ddl}

許可または拒否 [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ。

クエリを型に分割する方法を参照してください [上記](#permissions_for_queries).

可能な値:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

実行できない `SET allow_ddl = 1` もし `allow_ddl = 0` 現在のセッションの場合。

デフォルト値:1

[元の記事](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
