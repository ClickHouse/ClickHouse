---
description: 'Affiche les données du dictionnaire sous forme de table ClickHouse. Fonctionne de la même manière
  que le moteur Dictionary.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

Affiche les données du [dictionnaire](../statements/create/dictionary/overview.md) sous forme de table ClickHouse. Fonctionne de la même manière que le moteur [Dictionary](../../engines/table-engines/special/dictionary.md).

<div id="syntax">
  ## Syntaxe
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## Arguments
</div>

* `dict` — Nom du dictionnaire. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valeur retournée
</div>

Une table ClickHouse.

<div id="examples">
  ## Exemples
</div>

Table d’entrée `dictionary_source_table` :

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Créez un dictionnaire :

```sql title="Query"
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

```sql title="Query"
SELECT * FROM dictionary('new_dictionary');
```

```text title="Response"
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

<div id="related">
  ## Voir aussi
</div>

* [Moteur Dictionary](/fr/engines/table-engines/special/dictionary)