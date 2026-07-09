---
description: 'Документация по условию ALL'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'Условие ALL'
doc_type: 'reference'
---

Если в таблице есть несколько совпадающих строк, `ALL` возвращает их все. `SELECT ALL` эквивалентен `SELECT` без `DISTINCT`. Если указаны и `ALL`, и `DISTINCT`, будет сгенерировано исключение.

`ALL` можно указывать внутри агрегатных функций, хотя на результат запроса это практически не влияет.

Например:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

То же самое, что:

```sql
SELECT sum(number) FROM numbers(10);
```