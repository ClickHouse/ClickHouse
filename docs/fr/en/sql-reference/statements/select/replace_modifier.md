---
description: 'Documentation décrivant le modificateur APPLY, qui permet d''invoquer une fonction pour chaque ligne renvoyée par une expression de table externe d''une requête.'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'Modificateur REPLACE'
keywords: ['REPLACE', 'modifier']
doc_type: 'reference'
---

> Permet de spécifier un ou plusieurs [alias d&#39;expression](/fr/sql-reference/syntax#expression-aliases).

Chaque alias doit correspondre au nom d&#39;une colonne de l&#39;instruction `SELECT *`. Dans la liste des colonnes de sortie, la colonne correspondante
à l&#39;alias est remplacée par l&#39;expression de ce `REPLACE`.

Ce modificateur ne modifie ni les noms ni l&#39;ordre des colonnes. En revanche, il peut changer la valeur et le type de la valeur.

**Syntaxe :**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**Exemple :**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```