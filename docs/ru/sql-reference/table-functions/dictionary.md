---
toc_priority: 54
toc_title: dictionary
---

# dictionary {#dictionary-function}

Отображает данные [словаря](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) как таблицу ClickHouse. Работает аналогично движку [Dictionary](../../engines/table-engines/special/dictionary.md).

**Синтаксис**

``` sql
dictionary('dict')
```

**Аргументы** 

-   `dict` — имя словаря. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

Таблица ClickHouse.

**Пример**

Входная таблица `dictionary_source_table`:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Создаем словарь:

``` sql
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

Запрос:

``` sql
SELECT * FROM dictionary('new_dictionary');
```

Результат:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

**Смотрите также**

-   [Движок Dictionary](../../engines/table-engines/special/dictionary.md#dictionary)
