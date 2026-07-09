---
description: 'Документация по оператору DESCRIBE TABLE'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

Возвращает информацию о столбцах таблицы.

**Синтаксис**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Оператор `DESCRIBE` возвращает по одной строке для каждого столбца таблицы со следующими значениями типа [String](../../sql-reference/data-types/string.md):

* `name` — Имя столбца.
* `type` — Тип столбца.
* `default_type` — Выражение, используемое в [выражении по умолчанию](/ru/sql-reference/statements/create/table) столбца: `DEFAULT`, `MATERIALIZED` или `ALIAS`. Если выражение по умолчанию отсутствует, возвращается пустая строка.
* `default_expression` — Выражение, указанное после оператора `DEFAULT`.
* `comment` — [Комментарий столбца](/ru/sql-reference/statements/alter/column#comment-column).
* `codec_expression` — [Кодек](/ru/sql-reference/statements/create/table#column_compression_codec), применяемый к столбцу.
* `ttl_expression` — Выражение [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
* `is_subcolumn` — Флаг, равный `1` для внутренних подстолбцов. Он включается в результат, только если описание подстолбцов включено настройкой [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

Все столбцы в структурах данных [Nested](../../sql-reference/data-types/nested-data-structures/index.md) описываются отдельно. Имя каждого столбца имеет префикс в виде имени родительского столбца и точки.

Чтобы показать внутренние подстолбцы других типов данных, используйте настройку [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

**Пример**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Во втором запросе дополнительно показаны подстолбцы:

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

Оператор DESCRIBE также можно использовать с подзапросами и скалярными выражениями:

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

или

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

В этом случае возвращаются метаданные о результирующих столбцах указанного запроса или подзапроса. Это полезно для понимания структуры сложных запросов до их выполнения.

**См. также**

* настройка [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).