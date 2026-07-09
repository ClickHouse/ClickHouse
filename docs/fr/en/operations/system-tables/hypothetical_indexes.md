---
description: 'Table système répertoriant les index hypothétiques (« what-if ») définis dans la session actuelle'
keywords: ['table système', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

Répertorie tous les skip index hypothétiques (« what-if ») définis dans la session en cours. Voir [`CREATE HYPOTHETICAL INDEX`](/fr/sql-reference/statements/hypothetical-index#create-hypothetical-index) et [`EXPLAIN WHATIF`](/fr/sql-reference/statements/explain#explain-whatif).

Le contenu est limité à la session : chaque connexion ne voit que ses propres index hypothétiques, et la table est vide lorsqu&#39;aucun index n&#39;a été créé dans la session en cours.

Les `(database, table)` actuels sont résolus par UUID au moment de la requête ; ils reflètent donc `RENAME TABLE`, et les entrées correspondant à des tables supprimées sont masquées automatiquement.

<div id="columns">
  ## Colonnes
</div>

| Colonne       | Type     | Description                                                                      |
| ------------- | -------- | -------------------------------------------------------------------------------- |
| `database`    | `String` | Base de données cible.                                                           |
| `table`       | `String` | Table cible.                                                                     |
| `name`        | `String` | Nom de l’index.                                                                  |
| `type`        | `String` | Type d’index (`minmax`, `set`, `bloom_filter`, etc.).                            |
| `type_full`   | `String` | Expression du type d’index avec les arguments, par exemple `bloom_filter(0.01)`. |
| `expression`  | `String` | Expression de l’index telle qu’écrite dans `CREATE HYPOTHETICAL INDEX`.          |
| `granularity` | `UInt64` | Nombre de granules de données par granule d’index.                               |

<div id="example">
  ## Exemple
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` est le nom du type de base, et `type_full` inclut les arguments, ce qui permet aux utilisateurs de distinguer des variantes paramétrées comme `bloom_filter(0.01)` et `bloom_filter(0.001)`.

<div id="see-also">
  ## Voir aussi
</div>

* [`CREATE HYPOTHETICAL INDEX`](/fr/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/fr/sql-reference/statements/explain#explain-whatif)