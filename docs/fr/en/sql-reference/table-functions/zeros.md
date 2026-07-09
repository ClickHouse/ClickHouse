---
description: 'Utilisé à des fins de test comme la méthode la plus rapide pour générer de nombreuses lignes.
  Similaire aux tables système `system.zeros` et `system.zeros_mt`.'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – renvoie une table avec une unique colonne &#39;zero&#39; (UInt8) contenant l’entier 0 `N` fois
* `zeros_mt(N)` – identique à `zeros`, mais utilise plusieurs threads.

Cette fonction est utilisée à des fins de test comme la méthode la plus rapide pour générer de nombreuses lignes. Elle est similaire aux tables système `system.zeros` et `system.zeros_mt`.

Les requêtes suivantes sont équivalentes :

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```