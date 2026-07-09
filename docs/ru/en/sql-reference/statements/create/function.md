---
description: 'Документация по FUNCTION'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION — пользовательская функция (UDF)'
doc_type: 'reference'
---

Создаёт пользовательскую функцию (UDF) из lambda-выражения. Выражение должно состоять из параметров функции, констант, операторов или вызовов других функций.

**Синтаксис**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

Функция может иметь произвольное количество параметров.

Есть несколько ограничений:

* Имя функции должно быть уникальным среди пользовательских и системных функций.
* Рекурсивные функции не поддерживаются.
* Все переменные, используемые в функции, должны быть указаны в списке её параметров.

Если какое-либо из ограничений нарушено, возникает исключение.

**Пример**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

[Условная функция](../../../sql-reference/functions/conditional-functions.md) используется в пользовательской функции в следующем запросе:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

Замените существующую UDF:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## Материалы по теме
</div>

<div id="executable-udfs">
  ### [Исполняемые UDF](/ru/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [Пользовательские функции в ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
