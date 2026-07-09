---
description: 'Crée une table Merge temporaire. Le schéma est dérivé des tables sous-jacentes en utilisant une union de leurs colonnes et en déduisant des types communs.'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

Crée une table [Merge](../../engines/table-engines/special/merge.md) temporaire.
Le schéma de la table est dérivé des tables sous-jacentes en utilisant une union de leurs colonnes et en déduisant des types communs.
Les mêmes colonnes virtuelles que pour le moteur de table [Merge](../../engines/table-engines/special/merge.md) sont disponibles.

<div id="syntax">
  ## Syntaxe
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## Arguments
</div>

| Argument        | Description                                                                                                                                                                                                                                                                                                                                                             |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | Valeurs possibles (facultatif, valeur par défaut : `currentDatabase()`) :<br />    - nom de la base de données,<br />    - expression constante qui renvoie une chaîne contenant un nom de base de données, par exemple `currentDatabase()`,<br />    - `REGEXP(expression)`, où `expression` est une expression régulière correspondant aux noms des bases de données. |
| `tables_regexp` | Une expression régulière correspondant aux noms des tables dans la ou les bases de données spécifiées.                                                                                                                                                                                                                                                                  |

<div id="related">
  ## Voir aussi
</div>

* moteur de table [Merge](../../engines/table-engines/special/merge.md)