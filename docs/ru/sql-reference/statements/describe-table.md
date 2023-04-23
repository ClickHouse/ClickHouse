---
sidebar_position: 42
sidebar_label: DESCRIBE
---

# DESCRIBE TABLE {#misc-describe-table}

Возвращает описание столбцов таблицы.

**Синтаксис**

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Запрос `DESCRIBE` для каждого столбца таблицы возвращает строку со следующими значениями типа [String](../../sql-reference/data-types/string.md):

-   `name` — имя столбца;
-   `type` — тип столбца;
-   `default_type` — вид [выражения для значения по умолчанию](../../sql-reference/statements/create/table.md#create-default-values): `DEFAULT`, `MATERIALIZED` или `ALIAS`. Если значение по умолчанию не задано, то возвращается пустая строка;
-   `default_expression` — значение, заданное в секции `DEFAULT`;
-   `comment` — [комментарий](../../sql-reference/statements/alter/column.md#alter_comment-column);
-   `codec_expression` — [кодек](../../sql-reference/statements/create/table.md#codecs), который применяется к столбцу;
-   `ttl_expression` — выражение [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl);
-   `is_subcolumn` — флаг, который равен `1` для внутренних подстолбцов. Он появляется в результате, только если описание подстолбцов разрешено настройкой [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

Каждый столбец [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) структур описывается отдельно. Перед его именем ставится имя родительского столбца с точкой.
Чтобы отобразить внутренние подстолбцы других типов данных, нужно включить настройку [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns). 

**Пример**

Запрос:

``` sql
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

Результат:

``` text
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Второй запрос дополнительно выводит информацию о подстолбцах:

``` text
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

**См. также**

-   настройка [describe_include_subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).
