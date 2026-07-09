---
description: 'Exibe os dados do dicionário como uma tabela do ClickHouse. Funciona da mesma forma
  que o motor Dictionary.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

Exibe os dados do [dicionário](../statements/create/dictionary/overview.md) como uma tabela do ClickHouse. Funciona da mesma forma que o motor [Dictionary](../../engines/table-engines/special/dictionary.md).

<div id="syntax">
  ## Sintaxe
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## Argumentos
</div>

* `dict` — Nome de um dicionário. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela do ClickHouse.

<div id="examples">
  ## Exemplos
</div>

Tabela de entrada `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Crie um dicionário:

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

* [motor Dictionary](/pt-BR/engines/table-engines/special/dictionary)