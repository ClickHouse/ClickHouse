---
description: 'Документация по модификатору типа данных Nullable в ClickHouse'
sidebar_label: 'Nullable(T)'
sidebar_position: 44
slug: /sql-reference/data-types/nullable
title: 'Nullable(T)'
doc_type: 'reference'
---

Позволяет хранить специальный маркер ([NULL](../../sql-reference/syntax.md)), обозначающий «отсутствующее значение», наряду с обычными значениями, допустимыми для `T`. Например, столбец типа `Nullable(Int8)` может хранить значения типа `Int8`, а в строках без значения будет храниться `NULL`.

`T` не может быть ни одним из следующих составных типов данных:

* [Array](../../sql-reference/data-types/array.md) — Не поддерживается
* [Map](../../sql-reference/data-types/map.md) — Не поддерживается
* [Tuple](../../sql-reference/data-types/tuple.md) — Доступна бета-поддержка*

Однако составные типы данных **могут содержать** значения типа `Nullable`, например `Array(Nullable(Int8))` или `Tuple(Nullable(String), Nullable(Int64))`.

:::note Бета: Nullable Tuple

* [Nullable(Tuple(...))](../../sql-reference/data-types/tuple.md#nullable-tuple) поддерживается, если включен параметр `enable_nullable_tuple_type = 1`.
  :::

Поле типа `Nullable` не может быть включено в индексы таблицы.

`NULL` — значение по умолчанию для любого типа `Nullable`, если иное не указано в конфигурации сервера ClickHouse.

<div id="storage-features">
  ## Особенности хранения
</div>

Для хранения значений типа `Nullable` в столбце таблицы ClickHouse использует отдельный файл с масками `NULL` в дополнение к обычному файлу со значениями. Записи в файле масок позволяют ClickHouse различать `NULL` и значение по умолчанию соответствующего типа данных в каждой строке таблицы. Из-за дополнительного файла столбец `Nullable` занимает больше места в хранилище по сравнению с аналогичным обычным столбцом.

:::note
Использование `Nullable` почти всегда отрицательно сказывается на производительности, учитывайте это при проектировании баз данных.
:::

<div id="finding-null">
  ## Поиск NULL
</div>

Значения `NULL` в столбце можно находить с помощью подстолбца `null`, не читая весь столбец. Подстолбец возвращает `1`, если соответствующее значение равно `NULL`, и `0` в противном случае.

**Пример**

```sql title="Query"
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

```text title="Response"
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

<div id="usage-example">
  ## Пример использования
</div>

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

```sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

```sql
SELECT x + y FROM t_null
```

```text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```