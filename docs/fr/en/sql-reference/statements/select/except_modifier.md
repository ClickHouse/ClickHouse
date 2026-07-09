---
description: 'Documentation décrivant le modificateur EXCEPT, qui spécifie les noms d’une ou plusieurs colonnes à exclure du résultat. Toutes les colonnes dont le nom correspond sont omises du résultat.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except-modifier
title: 'Modificateur EXCEPT'
keywords: ['EXCEPT', 'modifier']
doc_type: 'reference'
---

> Spécifie les noms d’une ou plusieurs colonnes à exclure du résultat. Toutes les colonnes dont le nom correspond sont omises du résultat.

<div id="syntax">
  ## Syntaxe
</div>

```sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

<div id="examples">
  ## Exemples
</div>

```sql title="Query"
SELECT * EXCEPT (i) from columns_transformers;
```

```response title="Response"
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```