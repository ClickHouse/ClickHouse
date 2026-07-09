---
description: 'Используется для тестирования как самый быстрый способ генерировать большое количество строк.
  Аналогична системным таблицам `system.zeros` и `system.zeros_mt`.'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – возвращает таблицу с единственным столбцом &#39;zero&#39; (UInt8), содержащим целое число 0 `N` раз
* `zeros_mt(N)` – то же, что и `zeros`, но использует несколько потоков.

Эта функция используется для тестирования как самый быстрый способ генерировать большое количество строк. Аналогична системным таблицам `system.zeros` и `system.zeros_mt`.

Следующие запросы эквивалентны:

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```