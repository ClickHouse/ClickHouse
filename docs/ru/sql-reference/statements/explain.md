---
slug: /ru/sql-reference/statements/explain
sidebar_position: 39
sidebar_label: EXPLAIN
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

-   `header` — выводит выходной заголовок для шага. По умолчанию: 0.
-   `description` — выводит описание шага. По умолчанию: 1.
-   `indexes` — показывает используемые индексы, количество отфильтрованных кусков и гранул для каждого примененного индекса. По умолчанию: 0. Поддерживается для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
-   `actions` — выводит подробную информацию о действиях, выполняемых на данном шаге. По умолчанию: 0.
-   `json` — выводит шаги выполнения запроса в виде строки в формате [JSON](../../interfaces/formats.md#json). По умолчанию: 0. Чтобы избежать ненужного экранирования, рекомендуется использовать формат [TSVRaw](../../interfaces/formats.md#tabseparatedraw).

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

:::note Примечание
Оценка стоимости выполнения шага и запроса не поддерживается.
:::
При `json = 1` шаги выполнения запроса выводятся в формате JSON. Каждый узел — это словарь, в котором всегда есть ключи `Node Type` и `Plans`. `Node Type` — это строка с именем шага. `Plans` — это массив с описаниями дочерних шагов. Другие дополнительные ключи могут быть добавлены в зависимости от типа узла и настроек.

Пример:

```sql
EXPLAIN json = 1, description = 0 SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Union",
      "Plans": [
        {
          "Node Type": "Expression",
          "Plans": [
            {
              "Node Type": "SettingQuotaAndLimits",
              "Plans": [
                {
                  "Node Type": "ReadFromStorage"
                }
              ]
            }
          ]
        },
        {
          "Node Type": "Expression",
          "Plans": [
            {
              "Node Type": "SettingQuotaAndLimits",
              "Plans": [
                {
                  "Node Type": "ReadFromStorage"
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
```

При `description` = 1 к шагу добавляется ключ `Description`:

```json
{
  "Node Type": "ReadFromStorage",
  "Description": "SystemOne"
}
```

При `header` = 1 к шагу добавляется ключ `Header` в виде массива столбцов.

Пример:

```sql
EXPLAIN json = 1, description = 0, header = 1 SELECT 1, 2 + dummy;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Header": [
        {
          "Name": "1",
          "Type": "UInt8"
        },
        {
          "Name": "plus(2, dummy)",
          "Type": "UInt16"
        }
      ],
      "Plans": [
        {
          "Node Type": "SettingQuotaAndLimits",
          "Header": [
            {
              "Name": "dummy",
              "Type": "UInt8"
            }
          ],
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Header": [
                {
                  "Name": "dummy",
                  "Type": "UInt8"
                }
              ]
            }
          ]
        }
      ]
    }
  }
]
```

При `indexes` = 1 добавляется ключ `Indexes`. Он содержит массив используемых индексов. Каждый индекс описывается как строка в формате JSON с ключом `Type` (`MinMax`, `Partition`, `PrimaryKey` или `Skip`) и дополнительные ключи:

-   `Name` — имя индекса (на данный момент используется только для индекса `Skip`).
-   `Keys` — массив столбцов, используемых индексом.
-   `Condition` — строка с используемым условием.
-   `Description` — индекс (на данный момент используется только для индекса `Skip`).
-   `Parts` — количество кусков до/после применения индекса.
-   `Granules` — количество гранул до/после применения индекса.

Пример:

```json
"Node Type": "ReadFromMergeTree",
"Indexes": [
  {
    "Type": "MinMax",
    "Keys": ["y"],
    "Condition": "(y in [1, +inf))",
    "Parts": 5/4,
    "Granules": 12/11
  },
  {
    "Type": "Partition",
    "Keys": ["y", "bitAnd(z, 3)"],
    "Condition": "and((bitAnd(z, 3) not in [1, 1]), and((y in [1, +inf)), (bitAnd(z, 3) not in [1, 1])))",
    "Parts": 4/3,
    "Granules": 11/10
  },
  {
    "Type": "PrimaryKey",
    "Keys": ["x", "y"],
    "Condition": "and((x in [11, +inf)), (y in [1, +inf)))",
    "Parts": 3/2,
    "Granules": 10/6
  },
  {
    "Type": "Skip",
    "Name": "t_minmax",
    "Description": "minmax GRANULARITY 2",
    "Parts": 2/1,
    "Granules": 6/2
  },
  {
    "Type": "Skip",
    "Name": "t_set",
    "Description": "set GRANULARITY 2",
    "": 1/1,
    "Granules": 2/1
  }
]
```

При `actions` = 1 добавляются ключи, зависящие от типа шага.

Пример:

```sql
EXPLAIN json = 1, actions = 1, description = 0 SELECT 1 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Expression": {
        "Inputs": [],
        "Actions": [
          {
            "Node Type": "Column",
            "Result Type": "UInt8",
            "Result Type": "Column",
            "Column": "Const(UInt8)",
            "Arguments": [],
            "Removed Arguments": [],
            "Result": 0
          }
        ],
        "Outputs": [
          {
            "Name": "1",
            "Type": "UInt8"
          }
        ],
        "Positions": [0],
        "Project Input": true
      },
      "Plans": [
        {
          "Node Type": "SettingQuotaAndLimits",
          "Plans": [
            {
              "Node Type": "ReadFromStorage"
            }
          ]
        }
      ]
    }
  }
]
```

### EXPLAIN PIPELINE {#explain-pipeline}

Настройки:

-   `header` — выводит заголовок для каждого выходного порта. По умолчанию: 0.
-   `graph` — выводит граф, описанный на языке [DOT](https://ru.wikipedia.org/wiki/DOT_(язык)). По умолчанию: 0.
-   `compact` — выводит граф в компактном режиме, если включена настройка `graph`. По умолчанию: 1.

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
            NumbersRange × 2 0 → 1
```

### EXPLAIN ESTIMATE {#explain-estimate}

 Отображает оценки числа строк, засечек и кусков, которые будут прочитаны при выполнении запроса. Применяется для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree). 

**Пример**

Создадим таблицу:

```sql
CREATE TABLE ttt (i Int64) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity = 16, write_final_mark = 0;
INSERT INTO ttt SELECT number FROM numbers(128);
OPTIMIZE TABLE ttt;
```

Запрос:

```sql
EXPLAIN ESTIMATE SELECT * FROM ttt;
```

Результат:

```text
┌─database─┬─table─┬─parts─┬─rows─┬─marks─┐
│ default  │ ttt   │     1 │  128 │     8 │
└──────────┴───────┴───────┴──────┴───────┘
```
