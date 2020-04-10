---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# GenerateRandom {#table_engines-generate}

Механизм генерации случайных таблиц генерирует случайные данные для данной схемы таблиц.

Примеры употребления:

-   Используйте в тесте для заполнения воспроизводимого большого стола.
-   Генерируйте случайные входные данные для тестов размытия.

## Использование в сервере ClickHouse {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

То `max_array_length` и `max_string_length` параметры укажите максимальную длину всех
столбцы массива и строки соответственно в генерируемых данных.

Генерация таблицы движок поддерживает только `SELECT` запросы.

Он поддерживает все [Тип данных](../../../engines/table_engines/special/generate.md) это может быть сохранено в таблице за исключением `LowCardinality` и `AggregateFunction`.

**Пример:**

**1.** Настройка системы `generate_engine_table` стол:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Запрос данных:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Детали внедрения {#details-of-implementation}

-   Не поддерживаемый:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   Индексы
    -   Копирование

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
