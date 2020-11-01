---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "Autorisations pour les requ\xEAtes"
---

# Autorisations pour les requêtes {#permissions_for_queries}

Les requêtes dans ClickHouse peuvent être divisées en plusieurs types:

1.  Lire les requêtes de données: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Écrire des requêtes de données: `INSERT`, `OPTIMIZE`.
3.  Modifier les paramètres requête: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) requête: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

Les paramètres suivants règlent les autorisations utilisateur selon le type de requête:

-   [ReadOnly](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [allow_ddl](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` peut être réalisée avec tous les paramètres.

## ReadOnly {#settings_readonly}

Restreint les autorisations pour lire des données, écrire des données et modifier les requêtes de paramètres.

Voyez comment les requêtes sont divisées en types [surtout](#permissions_for_queries).

Valeurs possibles:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

Après le réglage de `readonly = 1` l'utilisateur ne peut pas changer `readonly` et `allow_ddl` les paramètres de la session en cours.

Lors de l'utilisation de la `GET` méthode dans le [Interface HTTP](../../interfaces/http.md), `readonly = 1` est définie automatiquement. Pour modifier les données, utilisez `POST` méthode.

Paramètre `readonly = 1` interdire à l'utilisateur de modifier tous les paramètres. Il y a un moyen d'interdire à l'utilisateur
de modifier uniquement des paramètres spécifiques, pour plus de détails, voir [contraintes sur les paramètres](constraints-on-settings.md).

Valeur par défaut: 0

## allow_ddl {#settings_allow_ddl}

Permet ou interdit [DDL](https://en.wikipedia.org/wiki/Data_definition_language) requête.

Voyez comment les requêtes sont divisées en types [surtout](#permissions_for_queries).

Valeurs possibles:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

Vous ne pouvez pas exécuter `SET allow_ddl = 1` si `allow_ddl = 0` pour la session en cours.

Valeur par défaut: 1

[Article Original](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
