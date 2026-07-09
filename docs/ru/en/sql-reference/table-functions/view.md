---
description: 'Преобразует подзапрос в таблицу. Функция реализует представления.'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
doc_type: 'reference'
---

Преобразует подзапрос в таблицу. Функция реализует представления (см. [CREATE VIEW](/ru/sql-reference/statements/create/view)). Полученная таблица не хранит данные, а только сохраняет указанный запрос `SELECT`. При чтении из таблицы ClickHouse выполняет этот запрос и удаляет из результата все ненужные столбцы.

<div id="syntax">
  ## Синтаксис
</div>

```sql
view(subquery)
```

<div id="arguments">
  ## Аргументы
</div>

* `subquery` — запрос `SELECT`.

<div id="returned_value">
  ## Возвращаемое значение
</div>

* Таблица.

<div id="examples">
  ## Примеры
</div>

Исходная таблица:

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

Вы можете использовать функцию `view` как параметр табличных функций [remote](/ru/sql-reference/table-functions/remote) и [cluster](/ru/sql-reference/table-functions/cluster):

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

<div id="related">
  ## См. также
</div>

* [Движок таблицы View](/ru/engines/table-engines/special/view/)