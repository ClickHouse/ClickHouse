---
toc_title: WHERE
---

# Секция WHERE {#select-where}

Позволяет задать выражение, которое ClickHouse использует для фильтрации данных перед всеми другими действиями в запросе кроме выражений, содержащихся в секции [PREWHERE](prewhere.md#prewhere-clause). Обычно, это выражение с логическими операторами.

Результат выражения должен иметь тип `UInt8`.

ClickHouse использует в выражении индексы, если это позволяет [движок таблицы](../../../engines/table-engines/index.md).

Если в секции необходимо проверить [NULL](../../../sql-reference/syntax.md#null-literal), то используйте операторы [IS NULL](../../operators/index.md#operator-is-null) и [IS NOT NULL](../../operators/index.md#is-not-null), а также соответствующие функции `isNull` и `isNotNull`. В противном случае выражение будет считаться всегда не выполненным.

Пример проверки на `NULL`:

``` sql
SELECT * FROM t_null WHERE y IS NULL
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

!!! note "Примечание"
    Существует оптимизация фильтрации под названием [prewhere](prewhere.md).

