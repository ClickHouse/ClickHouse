---
description: 'Muestra los datos del diccionario como una tabla de ClickHouse. Funciona
  igual que el motor Dictionary.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

Muestra los datos del [diccionario](../statements/create/dictionary/overview.md) como una tabla de ClickHouse. Funciona igual que el motor [Dictionary](../../engines/table-engines/special/dictionary.md).

<div id="syntax">
  ## Sintaxis
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## Argumentos
</div>

* `dict` — Nombre de un diccionario. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla de ClickHouse.

<div id="examples">
  ## Ejemplos
</div>

Tabla de entrada `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Crear un diccionario:

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
  ## Relacionados
</div>

* [motor Dictionary](/es/engines/table-engines/special/dictionary)