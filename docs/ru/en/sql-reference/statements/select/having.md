---
description: 'Документация по предложению HAVING'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'Предложение HAVING'
doc_type: 'reference'
---

Позволяет фильтровать результаты агрегации, полученные с помощью [GROUP BY](/ru/sql-reference/statements/select/group-by). Оно похоже на [WHERE](../../../sql-reference/statements/select/where.md), но разница в том, что `WHERE` применяется до агрегации, а `HAVING` — после неё.

В предложении `HAVING` можно ссылаться на результаты агрегации из предложения `SELECT` по их псевдониму. Кроме того, предложение `HAVING` позволяет фильтровать результаты дополнительных агрегатных функций, которые не возвращаются в результат запроса.

<div id="example">
  ## Пример
</div>

Если у вас есть таблица `sales` со следующей структурой:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

Вот как можно выполнить к нему запрос:

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

Здесь будут перечислены продавцы, у которых общий объём продаж в их регионе превышает 10 000.

<div id="limitations">
  ## Ограничения
</div>

`HAVING` нельзя использовать, если агрегация не выполняется. Вместо этого используйте предложение `WHERE`.