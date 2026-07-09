---
description: 'Documentation de l’instruction ALTER TABLE ... DELETE'
sidebar_label: 'DELETE'
sidebar_position: 39
slug: /sql-reference/statements/alter/delete
title: 'Instruction ALTER TABLE ... DELETE'
doc_type: 'reference'
---

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Supprime les données correspondant à l’expression de filtrage spécifiée. Implémenté sous forme de [mutation](/fr/sql-reference/statements/alter/index.md#mutations).

:::note
Le préfixe `ALTER TABLE` distingue cette syntaxe de celle de la plupart des autres systèmes prenant en charge SQL. Il indique que, contrairement à des requêtes similaires dans les bases de données OLTP, il s’agit d’une opération lourde qui n’est pas conçue pour être utilisée fréquemment. `ALTER TABLE` est considérée comme une opération lourde qui nécessite la fusion des données sous-jacentes avant leur suppression. Pour les tables MergeTree, envisagez plutôt d’utiliser la [requête `DELETE FROM`](/fr/sql-reference/statements/delete.md), qui effectue une suppression légère et peut être nettement plus rapide.
:::

Le `filter_expr` doit être de type `UInt8`. La requête supprime les lignes de la table pour lesquelles cette expression prend une valeur non nulle.

Une requête peut contenir plusieurs commandes séparées par des virgules.

Le caractère synchrone du traitement de la requête est défini par le paramètre [mutations&#95;sync](/fr/operations/settings/settings.md/#mutations_sync). Par défaut, il est asynchrone.

**Voir aussi**

* [Mutations](/fr/sql-reference/statements/alter/index.md#mutations)
* [Caractère synchrone des requêtes ALTER](/fr/sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
* Paramètre [mutations&#95;sync](/fr/operations/settings/settings.md/#mutations_sync)

<div id="related-content">
  ## Contenu connexe
</div>

* Article de blog : [Gérer les mises à jour et les suppressions dans ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)