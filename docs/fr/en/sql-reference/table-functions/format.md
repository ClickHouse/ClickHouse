---
description: 'Analyse les données des arguments selon le format d''entrée spécifié. Si l''argument de structure n''est pas spécifié, il est extrait des données.'
slug: /sql-reference/table-functions/format
sidebar_position: 65
sidebar_label: 'format'
title: 'format'
doc_type: 'reference'
---

Analyse les données des arguments selon le format d&#39;entrée spécifié. Si l&#39;argument de structure n&#39;est pas spécifié, il est extrait des données.

<div id="syntax">
  ## Syntaxe
</div>

```sql
format(format_name, [structure], data)
```

<div id="arguments">
  ## Arguments
</div>

* `format_name` — Le [format](/fr/sql-reference/formats) des données.
* `structure` - Structure de la table. Optionnelle. Format `column1_name column1_type, column2_name column2_type, ...`.
* `data` — Littéral de chaîne ou expression constante qui renvoie une chaîne contenant des données dans le format spécifié

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table contenant des données analysées à partir de l’argument `data`, selon le format spécifié et la structure spécifiée ou extraite.

<div id="examples">
  ## Exemples
</div>

Sans l’argument `structure` :

```sql title="Query"
SELECT * FROM format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌───b─┬─a─────┐
│ 111 │ Hello │
│ 123 │ World │
│ 112 │ Hello │
│ 124 │ World │
└─────┴───────┘
```

```sql title="Query"
DESC format(JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─name─┬─type──────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ b    │ Nullable(Float64) │              │                    │         │                  │                │
│ a    │ Nullable(String)  │              │                    │         │                  │                │
└──────┴───────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Avec l’argument `structure` :

```sql title="Query"
SELECT * FROM format(JSONEachRow, 'a String, b UInt32',
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
{"a": "Hello", "b": 112}
{"a": "World", "b": 124}
$$)
```

```response title="Response"
┌─a─────┬───b─┐
│ Hello │ 111 │
│ World │ 123 │
│ Hello │ 112 │
│ World │ 124 │
└───────┴─────┘
```

<div id="related">
  ## Voir aussi
</div>

* [Formats](../../interfaces/formats.md)