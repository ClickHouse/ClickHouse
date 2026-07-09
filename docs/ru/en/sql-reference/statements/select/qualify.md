---
description: 'Документация по предложению QUALIFY'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'Предложение QUALIFY'
doc_type: 'reference'
---

Позволяет фильтровать результаты оконных функций. Оно похоже на [предложение WHERE](../../../sql-reference/statements/select/where.md), но разница в том, что `WHERE` применяется до вычисления оконных функций, а `QUALIFY` — после.

В предложении `QUALIFY` можно ссылаться на результаты оконных функций из предложения `SELECT` по их псевдониму. Кроме того, предложение `QUALIFY` может фильтровать результаты дополнительных оконных функций, которые не возвращаются в итоговый результат запроса.

<div id="limitations">
  ## Ограничения
</div>

`QUALIFY` нельзя использовать, если в запросе нет оконных функций для вычисления. Вместо него используйте предложение `WHERE`.

<div id="examples">
  ## Примеры
</div>

Пример:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```