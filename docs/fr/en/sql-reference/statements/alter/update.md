---
description: 'Documentation de référence pour les instructions ALTER TABLE ... UPDATE'
sidebar_label: 'UPDATE'
sidebar_position: 40
slug: /sql-reference/statements/alter/update
title: 'Instructions ALTER TABLE ... UPDATE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_id] WHERE filter_expr
```

Modifie les données correspondant à l’expression de filtrage spécifiée. Cette opération est implémentée sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

:::note
Le préfixe `ALTER TABLE` distingue cette syntaxe de celle de la plupart des autres systèmes prenant en charge SQL. Il indique que, contrairement aux requêtes similaires dans les bases de données OLTP, il s’agit d’une opération lourde, qui n’est pas conçue pour être utilisée fréquemment.
:::

`filter_expr` doit être de type `UInt8`. Cette requête met à jour les valeurs des colonnes spécifiées avec celles des expressions correspondantes dans les lignes pour lesquelles `filter_expr` prend une valeur non nulle. Les valeurs sont converties vers le type de la colonne à l’aide de l’opérateur `CAST`. La mise à jour des colonnes utilisées dans le calcul de la clé primaire ou de la clé de partition n’est pas prise en charge.

Une requête peut contenir plusieurs commandes séparées par des virgules.

Le caractère synchrone du traitement de la requête est défini par le paramètre [mutations&#95;sync](/fr/operations/settings/settings.md/#mutations_sync). Par défaut, il est asynchrone.

**Voir aussi**

* [Mutations](/fr/sql-reference/statements/alter/index.md#mutations)
* [Caractère synchrone des requêtes ALTER](/fr/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* paramètre [mutations&#95;sync](/fr/operations/settings/settings.md/#mutations_sync)
* [Lightweight `UPDATE`](/fr/sql-reference/statements/update) - alternative légère de mise à jour utilisant des patch parts
* [`APPLY PATCHES`](/fr/sql-reference/statements/alter/apply-patches) - appliquer manuellement les patchs issus des lightweight updates

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Gestion des mises à jour et des suppressions dans ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)