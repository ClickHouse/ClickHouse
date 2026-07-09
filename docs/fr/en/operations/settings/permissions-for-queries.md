---
description: 'Paramètres des autorisations pour les requêtes.'
sidebar_label: 'Autorisations pour les requêtes'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: 'Autorisations pour les requêtes'
doc_type: 'référence'
---

Les requêtes dans ClickHouse peuvent être réparties en plusieurs types :

1. Requêtes de lecture de données : `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. Requêtes d’écriture de données : `INSERT`, `OPTIMIZE`.
3. Requêtes de modification des paramètres : `SET`, `USE`.
4. Requêtes [DDL](https://en.wikipedia.org/wiki/Data_definition_language) : `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

Les paramètres suivants régissent les autorisations des utilisateurs en fonction du type de requête :

<div id="readonly">
  ## readonly
</div>

Restreint les autorisations pour les requêtes de lecture de données, d&#39;écriture de données et de modification des paramètres.

Lorsqu&#39;il est défini sur 1, autorise :

* Tous les types de requêtes de lecture (comme SELECT et les requêtes équivalentes).
* Les requêtes qui modifient uniquement le contexte de session (comme USE).

Lorsqu&#39;il est défini sur 2, autorise ce qui précède, ainsi que :

* SET et CREATE TEMPORARY TABLE

  :::tip
  Les requêtes comme EXISTS, DESCRIBE, EXPLAIN, SHOW PROCESSLIST, etc. sont équivalentes à SELECT, car elles effectuent simplement des SELECT sur des tables système.
  :::

Valeurs possibles :

* 0 — Les requêtes de lecture, d&#39;écriture et de modification des paramètres sont autorisées.
* 1 — Seules les requêtes de lecture de données sont autorisées.
* 2 — Les requêtes de lecture de données et de modification des paramètres sont autorisées.

Valeur par défaut : 0

:::note
Après avoir défini `readonly = 1`, l&#39;utilisateur ne peut pas modifier les paramètres `readonly` et `allow_ddl` dans la session en cours.

Lors de l&#39;utilisation de la méthode `GET` dans l&#39;[interface HTTP](/fr/interfaces/http), `readonly = 1` est défini automatiquement. Pour modifier des données, utilisez la méthode `POST`.

Le paramètre `readonly = 1` empêche l&#39;utilisateur de modifier les paramètres. Il est possible d&#39;empêcher l&#39;utilisateur de modifier uniquement certains paramètres. Il est également possible d&#39;autoriser la modification de certains paramètres uniquement sous les restrictions de `readonly = 1`. Pour plus de détails, voir [contraintes sur les paramètres](../../operations/settings/constraints-on-settings.md).
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

Autorise ou interdit les requêtes [DDL](https://en.wikipedia.org/wiki/Data_definition_language).

Valeurs possibles :

* 0 — les requêtes DDL ne sont pas autorisées.
* 1 — les requêtes DDL sont autorisées.

Valeur par défaut : 1

:::note
Vous ne pouvez pas exécuter `SET allow_ddl = 1` si `allow_ddl = 0` dans la session en cours.
:::

:::note KILL QUERY
`KILL QUERY` peut être exécuté quelle que soit la combinaison des paramètres readonly et allow&#95;ddl.
:::