---
description: 'Документация по оператору `EXISTS`'
slug: /sql-reference/operators/exists
title: 'EXISTS'
doc_type: 'reference'
---

Оператор `EXISTS` проверяет, есть ли записи в результате подзапроса. Если результат пуст, оператор возвращает `0`. В противном случае он возвращает `1`.

`EXISTS` также можно использовать в предложении [WHERE](../../sql-reference/statements/select/where.md).

:::tip
Ссылки на таблицы и столбцы основного запроса в подзапросе не поддерживаются.
:::

**Синтаксис**

```sql
EXISTS(subquery)
```

**Пример**

Запрос с проверкой наличия значений в подзапросе:

```sql title="Query"
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

Запрос с подзапросом, который возвращает несколько строк:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

Запрос с подзапросом, возвращающим пустой результат:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```