---
description: 'Transforme une sous-requête en une table. Cette fonction implémente des vues.'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
doc_type: 'référence'
---

Transforme une sous-requête en une table. Cette fonction implémente des vues (voir [CREATE VIEW](/fr/sql-reference/statements/create/view)). La table résultante ne stocke pas de données ; elle stocke uniquement la requête `SELECT` spécifiée. Lors de la lecture de la table, ClickHouse exécute la requête et supprime du résultat toutes les colonnes inutiles.

<div id="syntax">
  ## Syntaxe
</div>

```sql
view(subquery)
```

<div id="arguments">
  ## Arguments
</div>

* `subquery` — requête `SELECT`.

<div id="returned_value">
  ## Valeur renvoyée
</div>

* Une table.

<div id="examples">
  ## Exemples
</div>

Table d’entrée :

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

Vous pouvez utiliser la fonction `view` en tant que paramètre des fonctions de table [remote](/fr/sql-reference/table-functions/remote) et [cluster](/fr/sql-reference/table-functions/cluster) :

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

<div id="related">
  ## Voir aussi
</div>

* [Moteur de table View](/fr/engines/table-engines/special/view/)