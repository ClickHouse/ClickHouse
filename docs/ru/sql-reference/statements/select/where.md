---
slug: /ru/sql-reference/statements/select/where
sidebar_label: WHERE
---

# Секция WHERE {#select-where}

Позволяет задать выражение, которое ClickHouse использует для фильтрации данных перед всеми другими действиями в запросе кроме выражений, содержащихся в секции [PREWHERE](prewhere.md#prewhere-clause). Обычно это выражение с логическими операторами.

Результат выражения должен иметь тип `UInt8`.

ClickHouse использует в выражении индексы, если это позволяет [движок таблицы](../../../engines/table-engines/index.md).

:::note Примечание
Существует оптимизация фильтрации под названием [PREWHERE](prewhere.md).
:::
Если в секции необходимо проверить [NULL](../../../sql-reference/syntax.md#null-literal), то используйте операторы [IS NULL](../../operators/index.md#operator-is-null) и [IS NOT NULL](../../operators/index.md#is-not-null), а также соответствующие функции [isNull](../../../sql-reference/functions/functions-for-nulls.md#isnull) и [isNotNull](../../../sql-reference/functions/functions-for-nulls.md#isnotnull). В противном случае выражение будет считаться всегда не выполненным.

**Пример**

Чтобы найти числа, которые кратны 3 и больше 10, можно выполнить запрос к [таблице numbers](../../../sql-reference/table-functions/numbers.md):

``` sql
SELECT number FROM numbers(20) WHERE (number > 10) AND (number % 3 == 0);
```

Результат:

``` text
┌─number─┐
│     12 │
│     15 │
│     18 │
└────────┘
```

Пример проверки на `NULL`:

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

Результат:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```
