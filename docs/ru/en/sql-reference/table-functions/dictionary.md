---
description: 'Отображает данные словаря в виде таблицы ClickHouse. Работает так же,
  как движок Dictionary.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

Отображает данные [словаря](../statements/create/dictionary/overview.md) в виде таблицы ClickHouse. Работает так же, как [движок Dictionary](../../engines/table-engines/special/dictionary.md).

<div id="syntax">
  ## Синтаксис
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## Аргументы
</div>

* `dict` — Имя словаря. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица ClickHouse.

<div id="examples">
  ## Примеры
</div>

Исходная таблица `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Создайте словарь:

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
  ## См. также
</div>

* [Движок Dictionary](/ru/engines/table-engines/special/dictionary)