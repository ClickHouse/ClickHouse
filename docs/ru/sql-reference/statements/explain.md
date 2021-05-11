---
toc_priority: 39
toc_title: EXPLAIN
---

# EXPLAIN {#explain}

Выводит план выполнения запроса.

Синтаксис:

```sql
EXPLAIN [AST | SYNTAX | PLAN | PIPELINE] [setting = value, ...] SELECT ... [FORMAT ...]
```

Пример:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV;
```

```sql
Union
  Expression (Projection)
    Expression (Before ORDER BY and SELECT)
      Aggregating
        Expression (Before GROUP BY)
          SettingQuotaAndLimits (Set limits and quota after reading from storage)
            ReadFromStorage (SystemNumbers)
  Expression (Projection)
    MergingSorted (Merge sorted streams for ORDER BY)
      MergeSorting (Merge sorted blocks for ORDER BY)
        PartialSorting (Sort each block for ORDER BY)
          Expression (Before ORDER BY and SELECT)
            Aggregating
              Expression (Before GROUP BY)
                SettingQuotaAndLimits (Set limits and quota after reading from storage)
                  ReadFromStorage (SystemNumbers)
```

## Типы EXPLAIN {#explain-types}

-  `AST` — абстрактное синтаксическое дерево.
-  `SYNTAX` — текст запроса после оптимизации на уровне AST.
-  `PLAN` — план выполнения запроса.
-  `PIPELINE` — конвейер выполнения запроса.

### EXPLAIN AST {#explain-ast}

Дамп AST запроса. Поддерживает все типы запросов, не только `SELECT`.

Примеры:

```sql
EXPLAIN AST SELECT 1;
```

```sql
SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 1)
   ExpressionList (children 1)
    Literal UInt64_1
```

```sql
EXPLAIN AST ALTER TABLE t1 DELETE WHERE date = today();
```

```sql
  explain
  AlterQuery  t1 (children 1)
   ExpressionList (children 1)
    AlterCommand 27 (children 1)
     Function equals (children 1)
      ExpressionList (children 2)
       Identifier date
       Function today (children 1)
        ExpressionList
```

### EXPLAIN SYNTAX {#explain-syntax}

Возвращает текст запроса после применения синтаксических оптимизаций.

Пример:

```sql
EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c;
```

```sql
SELECT
    `--a.number` AS `a.number`,
    `--b.number` AS `b.number`,
    number AS `c.number`
FROM
(
    SELECT
        number AS `--a.number`,
        b.number AS `--b.number`
    FROM system.numbers AS a
    CROSS JOIN system.numbers AS b
) AS `--.s`
CROSS JOIN system.numbers AS c
```

### EXPLAIN PLAN {#explain-plan}

Дамп шагов выполнения запроса.

Настройки:

-  `header` — выводит выходной заголовок для шага. По умолчанию: 0.
-  `description` — выводит описание шага. По умолчанию: 1.
-  `actions` — выводит подробную информацию о действиях, выполняемых на данном шаге. По умолчанию: 0.

Пример:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number % 4;
```

```sql
Union
  Expression (Projection)
  Expression (Before ORDER BY and SELECT)
    Aggregating
      Expression (Before GROUP BY)
        SettingQuotaAndLimits (Set limits and quota after reading from storage)
          ReadFromStorage (SystemNumbers)
```

!!! note "Примечание"
    Оценка стоимости выполнения шага и запроса не поддерживается.

### EXPLAIN PIPELINE {#explain-pipeline}

Настройки:

-   `header` — выводит заголовок для каждого выходного порта. По умолчанию: 0.
-   `graph` — выводит граф, описанный на языке [DOT](https://ru.wikipedia.org/wiki/DOT_(язык)). По умолчанию: 0.
-   `compact` — выводит граф в компактном режиме, если включена настройка `graph`. По умолчанию: 1.
-   `indexes` — показывает используемые индексы, количество отфильтрованных кусков и гранул для каждого примененного индекса. По умолчанию: 0. Поддерживается для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Пример:

```sql
EXPLAIN PIPELINE SELECT sum(number) FROM numbers_mt(100000) GROUP BY number % 4;
```

```sql
(Union)
(Expression)
ExpressionTransform
  (Expression)
  ExpressionTransform
    (Aggregating)
    Resize 2 → 1
      AggregatingTransform × 2
        (Expression)
        ExpressionTransform × 2
          (SettingQuotaAndLimits)
            (ReadFromStorage)
            NumbersMt × 2 0 → 1
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/explain/) <!--hide-->
