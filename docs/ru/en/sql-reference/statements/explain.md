---
description: 'Документация по EXPLAIN'
sidebar_label: 'EXPLAIN'
sidebar_position: 39
slug: /sql-reference/statements/explain
title: 'Оператор EXPLAIN'
doc_type: 'reference'
---

Отображает план выполнения оператора.

<div class="vimeo-container">
  <iframe
    src="//www.youtube.com/embed/hP6G2Nlz_cA"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
fullscreen;
picture-in-picture"
    allowfullscreen
  />
</div>

Синтаксис:

```sql
EXPLAIN [AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE | TABLE OVERRIDE | WHATIF] [setting = value, ...]
    [
      SELECT ... |
      tableFunction(...) [COLUMNS (...)] [ORDER BY ...] [PARTITION BY ...] [PRIMARY KEY] [SAMPLE BY ...] [TTL ...]
    ]
    [FORMAT ...]
```

Пример:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV;
```

```sql
Output: sum(number)

Union
├──Aggregating
│  │  Keys:
│  │  Aggregates: sum(number)
│  │  Skip merging: 0
│  └──ReadFromSystemNumbers
│        Output: number
└──Sorting (Sorting for ORDER BY)
   │  Sort description: sum(number) ASC
   └──Aggregating
      │  Keys:
      │  Aggregates: sum(number)
      │  Skip merging: 0
      └──ReadFromSystemNumbers
            Output: number
```

<div id="explain-types">
  ## Типы EXPLAIN
</div>

* `AST` — Абстрактное синтаксическое дерево.
* `SYNTAX` — Текст запроса после оптимизаций на уровне AST.
* `QUERY TREE` — Дерево запроса после оптимизаций на уровне Query Tree.
* `PLAN` — План выполнения запроса.
* `PIPELINE` — Конвейер выполнения запроса.

<div id="explain-ast">
  ### EXPLAIN AST
</div>

Вывод AST запроса. Поддерживает все типы запросов, а не только `SELECT`.

Настройки:

* `graph` – Выводит AST в виде графа, описанного на языке описания графов [DOT](https://en.wikipedia.org/wiki/DOT_\(graph_description_language\)). По умолчанию: 0.

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

<div id="explain-syntax">
  ### EXPLAIN SYNTAX
</div>

Показывает абстрактное синтаксическое дерево (AST) запроса после синтаксического анализа.

Это делается путём разбора запроса, построения AST запроса и дерева запроса, при необходимости запуска анализатора запросов и проходов оптимизации, а затем преобразования дерева запроса обратно в AST запроса.

Настройки:

* `oneline` – Вывести запрос в одну строку. По умолчанию: `0`.
* `run_query_tree_passes` – Запустить проходы дерева запроса перед выводом дерева запроса. По умолчанию: `0`.
* `query_tree_passes` – Если задано `run_query_tree_passes`, указывает, сколько проходов нужно выполнить. Если `query_tree_passes` не указано, выполняются все проходы.

Примеры:

```sql title="Query"
EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number;
```

```sql title="Response"
SELECT *
FROM system.numbers AS a, system.numbers AS b, system.numbers AS c
WHERE (a.number = b.number) AND (b.number = c.number)
```

С параметром `run_query_tree_passes`:

```sql title="Query"
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number;
```

```sql title="Response"
SELECT
    __table1.number AS `a.number`,
    __table2.number AS `b.number`,
    __table3.number AS `c.number`
FROM system.numbers AS __table1
ALL INNER JOIN system.numbers AS __table2 ON __table1.number = __table2.number
ALL INNER JOIN system.numbers AS __table3 ON __table2.number = __table3.number
```

<div id="explain-query-tree">
  ### EXPLAIN QUERY TREE
</div>

Настройки:

* `run_passes` — Выполнить все проходы дерева запроса перед выводом дерева запроса. По умолчанию: `1`.
* `dump_passes` — Выводить сведения об использованных проходах перед выводом дерева запроса. По умолчанию: `0`.
* `passes` — Указывает, сколько проходов нужно выполнить. Если установлено значение `-1`, выполняются все проходы. По умолчанию: `-1`.
* `dump_tree` — Показывать дерево запроса. По умолчанию: `1`.
* `dump_ast` — Показывать AST запроса, построенное из дерева запроса. По умолчанию: `0`.

Пример:

```sql
EXPLAIN QUERY TREE SELECT id, value FROM test_table;
```

```sql
QUERY id: 0
  PROJECTION COLUMNS
    id UInt64
    value String
  PROJECTION
    LIST id: 1, nodes: 2
      COLUMN id: 2, column_name: id, result_type: UInt64, source_id: 3
      COLUMN id: 4, column_name: value, result_type: String, source_id: 3
  JOIN TREE
    TABLE id: 3, table_name: default.test_table
```

<div id="explain-plan">
  ### EXPLAIN PLAN
</div>

Выводит шаги плана запроса.

Настройки:

* `optimize` — Управляет тем, применять ли оптимизации плана запроса перед его отображением. По умолчанию: 1.
* `header` — Выводит заголовок для шага. По умолчанию: 0.
* `description` — Выводит описание шага. По умолчанию: 1.
* `indexes` — Показывает используемые индексы, количество отфильтрованных частей и количество отфильтрованных гранул для каждого применённого индекса. По умолчанию: 0. Поддерживается для таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Начиная с ClickHouse &gt;= v25.9, этот оператор показывает осмысленный результат только при использовании с `SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0`.
* `projections` — Показывает все проанализированные проекции и их влияние на фильтрацию на уровне частей на основе условий по первичному ключу проекции. Для каждой проекции в этом разделе приводится статистика, например количество частей, строк, marks и диапазонов, которые были оценены с использованием первичного ключа проекции. Также показывается, сколько частей данных было пропущено благодаря этой фильтрации, без чтения из самой проекции. Определить, использовалась ли проекция для чтения фактически или только анализировалась для фильтрации, можно по полю `description`. По умолчанию: 0. Поддерживается для таблиц [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
* `actions` — Выводит подробную информацию о действиях шага. По умолчанию: 1.
* `sorting` — Выводит описание сортировки для каждого шага плана, который формирует отсортированный вывод. По умолчанию: 0.
* `keep_logical_steps` — Сохраняет логические шаги плана для JOIN вместо преобразования их в физические реализации JOIN. По умолчанию: 0.
* `json` — Выводит шаги плана запроса как строку в формате [JSON](/ru/interfaces/formats/JSON). По умолчанию: 0. Чтобы избежать лишнего экранирования, рекомендуется использовать формат [TabSeparatedRaw (TSVRaw)](/ru/interfaces/formats/TabSeparatedRaw).
* `input_headers` — Выводит входные заголовки для шага. По умолчанию: 0. В основном полезно только разработчикам для отладки проблем, связанных с несоответствием входных и выходных заголовков.
* `column_structure` — Также выводит структуру столбцов в заголовках помимо их имени и типа. По умолчанию: 0. В основном полезно только разработчикам для отладки проблем, связанных с несоответствием входных и выходных заголовков.
* `distributed` — Показывает планы запроса, выполняемые на удалённых узлах для distributed таблиц или параллельных реплик. Не поддерживается вместе с `json`. По умолчанию: 0.
* `compact` — Если включено, скрывает из плана шаги выражений и подробную информацию о действиях (входы, функции, псевдонимы и позиции вывода). Действует только при `actions = 1`. По умолчанию: 1.
* `pretty` — Выводит дерево плана с использованием символов рисования линий (├──, └──, │) вместо отступов для наглядного отображения иерархии. Также форматирует свойства шага JOIN в одну строку. По умолчанию: 1.

:::note
По умолчанию `explain_query_plan_default = 'pretty'`, поэтому `actions`, `compact` и `pretty` инициализируются значением `1`, а план отображается в компактном и наглядном виде с аннотациями действий. Явное указание любого из этих параметров в операторе `EXPLAIN` (например, `EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT ...`) всегда переопределяет значение по умолчанию.

До ClickHouse 26.7 значениями по умолчанию для `actions`, `compact` и `pretty` были `0`. Вы по-прежнему можете получить такой вывод, установив `explain_query_plan_default = 'legacy'` (глобально или в `SETTINGS` для конкретного запроса) либо задав `compatibility` равным любой версии старше `26.7`.

Параметры `json` и `distributed` не включают значения по умолчанию для `pretty` (`actions`, `compact` и `pretty`), даже если `explain_query_plan_default = 'pretty'`. Чтобы добавить в их вывод подробную информацию о действиях, задайте `actions = 1` вручную.
:::

Пример:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number % 4  LIMIT 1;
```

```sql
Output: sum(number)

Limit (preliminary LIMIT)
│  Limit 1
│  Offset 0
└──Aggregating
   │  Keys: number MOD 4
   │  Aggregates: sum(number)
   │  Skip merging: 0
   └──ReadFromSystemNumbers
         Output: number
```

:::note
Оценка стоимости шагов и запросов не поддерживается.
:::

Когда `json = 1`, план запроса представлен в формате JSON. Каждый узел — это словарь, который всегда содержит ключи `Node Type`, `Node Id` и `Plans`. `Node Type` — строка с именем шага, а `Node Id` — уникальный идентификатор шага (имя шага с числовым суффиксом, например `Union_10`). `Plans` — массив с описаниями дочерних шагов. В зависимости от типа узла и настроек могут добавляться и другие необязательные ключи.

Пример:

```sql
EXPLAIN json = 1, description = 0 SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Union",
      "Node Id": "Union_10",
      "Plans": [
        {
          "Node Type": "Expression",
          "Node Id": "Expression_13",
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Node Id": "ReadFromStorage_0"
            }
          ]
        },
        {
          "Node Type": "Expression",
          "Node Id": "Expression_16",
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Node Id": "ReadFromStorage_4"
            }
          ]
        }
      ]
    }
  }
]
```

При `description` = 1 в шаг добавляется ключ `Description`:

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
      "Node Id": "Expression_5",
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
          "Node Type": "ReadFromStorage",
          "Node Id": "ReadFromStorage_0",
          "Header": [
            {
              "Name": "dummy",
              "Type": "UInt8"
            }
          ]
        }
      ]
    }
  }
]
```

При `indexes` = 1 добавляется ключ `Indexes`. Он содержит массив использованных индексов. Каждый индекс описывается в виде JSON-объекта с ключом `Type` (строка `Partition Min-Max`, `Partition`, `Statistics`, `PrimaryKey` или `Skip`) и необязательными ключами:

* `Name` — Имя индекса (в настоящее время используется только для индексов `Skip`).
* `Keys` — Массив столбцов, используемых индексом.
* `Condition` — Используемое условие.
* `Description` — Описание индекса (в настоящее время используется только для индексов `Skip`).
* `Parts` — Количество частей после/до применения индекса.
* `Granules` — Количество гранул после/до применения индекса.
* `Ranges` — Количество диапазонов гранул после применения индекса.

Пример:

```json
"Node Type": "ReadFromMergeTree",
"Indexes": [
  {
    "Type": "Partition Min-Max",
    "Keys": ["y"],
    "Condition": "(y in [1, +inf))",
    "Parts": 4/5,
    "Granules": 11/12
  },
  {
    "Type": "Partition",
    "Keys": ["y", "bitAnd(z, 3)"],
    "Condition": "and((bitAnd(z, 3) not in [1, 1]), and((y in [1, +inf)), (bitAnd(z, 3) not in [1, 1])))",
    "Parts": 3/4,
    "Granules": 10/11
  },
  {
    "Type": "PrimaryKey",
    "Keys": ["x", "y"],
    "Condition": "and((x in [11, +inf)), (y in [1, +inf)))",
    "Parts": 2/3,
    "Granules": 6/10,
    "Search Algorithm": "generic exclusion search"
  },
  {
    "Type": "Skip",
    "Name": "t_minmax",
    "Description": "minmax GRANULARITY 2",
    "Parts": 1/2,
    "Granules": 2/6
  },
  {
    "Type": "Skip",
    "Name": "t_set",
    "Description": "set GRANULARITY 2",
    "": 1/1,
    "Granules": 1/2
  }
]
```

При `projections` = 1 добавляется ключ `Projections`. Он содержит массив проанализированных проекций. Каждая проекция описывается в формате JSON со следующими ключами:

* `Name` — Имя проекции.
* `Condition` — Используемое условие по первичному ключу проекции.
* `Description` — Описание того, как используется проекция (например, фильтрация на уровне частей).
* `Selected Parts` — Число частей, отобранных проекцией.
* `Selected Marks` — Число отобранных меток.
* `Selected Ranges` — Число отобранных диапазонов.
* `Selected Rows` — Число отобранных строк.
* `Filtered Parts` — Число частей, пропущенных за счёт фильтрации на уровне частей.

Пример:

```json
"Node Type": "ReadFromMergeTree",
"Projections": [
  {
    "Name": "region_proj",
    "Description": "Projection has been analyzed and is used for part-level filtering",
    "Condition": "(region in ['us_west', 'us_west'])",
    "Search Algorithm": "binary search",
    "Selected Parts": 3,
    "Selected Marks": 3,
    "Selected Ranges": 3,
    "Selected Rows": 3,
    "Filtered Parts": 2
  },
  {
    "Name": "user_id_proj",
    "Description": "Projection has been analyzed and is used for part-level filtering",
    "Condition": "(user_id in [107, 107])",
    "Search Algorithm": "binary search",
    "Selected Parts": 1,
    "Selected Marks": 1,
    "Selected Ranges": 1,
    "Selected Rows": 1,
    "Filtered Parts": 2
  }
]
```

При `actions` = 1 добавляемые ключи зависят от типа шага.

Пример:

```sql
EXPLAIN json = 1, actions = 1, description = 0 SELECT 1 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Node Id": "Expression_5",
      "Expression": {
        "Inputs": [
          {
            "Name": "dummy",
            "Type": "UInt8"
          }
        ],
        "Actions": [
          {
            "Node Type": "INPUT",
            "Result Type": "UInt8",
            "Result Name": "dummy",
            "Arguments": [0],
            "Removed Arguments": [0],
            "Result": 0
          },
          {
            "Node Type": "COLUMN",
            "Result Type": "UInt8",
            "Result Name": "1",
            "Column": "Const(UInt8)",
            "Arguments": [],
            "Removed Arguments": [],
            "Result": 1
          }
        ],
        "Outputs": [
          {
            "Name": "1",
            "Type": "UInt8"
          }
        ],
        "Positions": [1]
      },
      "Plans": [
        {
          "Node Type": "ReadFromStorage",
          "Node Id": "ReadFromStorage_0"
        }
      ]
    }
  }
]
```

При `compact = 0` и `actions = 1` отображаются шаги `Expression` вместе с подробной информацией о выражениях:

```sql
EXPLAIN actions = 1, compact = 0 SELECT sum(number) FROM numbers(10) GROUP BY number % 4;
```

```text
Output: sum(number)

Expression ((Project names + Projection))
│  Actions: INPUT : 0 -> sum(__table1.number) UInt64 : 0
│           INPUT :: 1 -> modulo(__table1.number, 4_UInt8) UInt8 : 1
│           ALIAS sum(__table1.number) :: 0 -> sum(number) UInt64 : 2
│  Positions: 2
└──Aggregating
   │  Keys: number MOD 4
   │  Aggregates: sum(number)
   │  Skip merging: 0
   └──Expression ((Before GROUP BY + Change column names to column identifiers))
      │  Actions: INPUT : 0 -> number UInt64 : 0
      │           COLUMN Const(UInt8) -> 4_UInt8 UInt8 : 1
      │           ALIAS number :: 0 -> __table1.number UInt64 : 2
      │           FUNCTION modulo(__table1.number : 2, 4_UInt8 :: 1) -> modulo(__table1.number, 4_UInt8) UInt8 : 0
      │  Positions: 0 2
      └──ReadFromSystemNumbers
            Output: number
```

При `distributed` = 1 вывод включает не только локальный план запроса, но и планы запросов, которые будут выполняться на удалённых узлах. Это полезно для анализа и отладки распределённых запросов.

:::note
`distributed` отображается только в устаревшей (не-`pretty`) форме, поскольку вывод `pretty` не включает планы удалённых сегментов в дерево плана. По этой причине включение `distributed` автоматически отключает значения по умолчанию `pretty` (`actions`, `compact` и `pretty`), независимо от `explain_query_plan_default`. Вы по-прежнему можете вручную задать `actions=1`. Параметр `distributed` также не поддерживается совместно с `json`.
:::

Пример с distributed таблицей:

```sql
EXPLAIN distributed=1 SELECT * FROM remote('127.0.0.{1,2}', numbers(2)) WHERE number = 1;
```

```sql
Union
  Expression ((Project names + (Projection + (Change column names to column identifiers + (Project names + Projection)))))
    Filter ((WHERE + Change column names to column identifiers))
      ReadFromSystemNumbers
  Expression ((Project names + (Projection + Change column names to column identifiers)))
    ReadFromRemote (Read from remote replica)
      Expression ((Project names + Projection))
        Filter ((WHERE + Change column names to column identifiers))
          ReadFromSystemNumbers
```

Пример с параллельными репликами:

```sql
SET enable_parallel_replicas = 2, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'default';

EXPLAIN distributed=1 SELECT sum(number) FROM test_table GROUP BY number % 4;
```

```sql
Expression ((Project names + Projection))
  MergingAggregated
    Union
      Aggregating
        Expression ((Before GROUP BY + Change column names to column identifiers))
          ReadFromMergeTree (default.test_table)
      ReadFromRemoteParallelReplicas
        BlocksMarshalling
          Aggregating
            Expression ((Before GROUP BY + Change column names to column identifiers))
              ReadFromMergeTree (default.test_table)
```

В обоих примерах план запроса отображает полный поток выполнения, включая локальные и удалённые этапы.

При `pretty` = 1 дерево плана отображается с использованием символов псевдографики вместо отступов, а для ключевых шагов показывается дополнительная информация:

* **Выходные столбцы запроса** выводятся в верхней части плана.
* **Выражения** в фильтрах, ключах агрегации, описаниях сортировки и оконных функциях отображаются в SQL-подобной нотации, удобной для чтения (например, `a + 1 > 5` вместо `greater(plus(a, 1), 5)`). Внутренние префиксы идентификаторов столбцов (например, `__table1.`) для наглядности убираются.
* **Шаги источника** (например, `ReadFromMergeTree`) отображают свои выходные столбцы.
* **Шаги фильтрации** отображают условие фильтрации в SQL-нотации. Если присутствуют runtime-фильтры JOIN, они показываются отдельно.
* **Шаги агрегации** отображают ключи и агрегатные функции с их аргументами (например, `sum(c)`, `count()`).
* **Множества IN** из литералов Tuple показывают свои значения (для больших множеств — в усечённом виде), множества на основе подзапросов помечаются как `subquery1`, `subquery2` и т. д., а множества из таблиц с движком `Set` показывают имя таблицы.
* **Шаги JOIN** отображают отношение JOIN с использованием математической нотации, оценочное число строк в результате,
  а также то, какие выходные столбцы берутся из левой и правой стороны. Для
  обозначения различных типов JOIN используются следующие символы:

| Символ                 | Тип JOIN        |
| ---------------------- | --------------- |
| `⋈`                    | INNER JOIN      |
| `⟕`                    | LEFT JOIN       |
| `⟖`                    | RIGHT JOIN      |
| `⟗`                    | FULL JOIN       |
| `⋉`                    | LEFT SEMI JOIN  |
| `⋊`                    | RIGHT SEMI JOIN |
| `⋉` with strikethrough | LEFT ANTI JOIN  |
| `⋊` with strikethrough | RIGHT ANTI JOIN |
| `×`                    | CROSS JOIN      |

Например, `t1 ⟕ t2` означает LEFT JOIN между таблицами `t1` и `t2`.
Число в скобках после имени таблицы (например, `t1[100]`) указывает на оценочное количество строк,
если доступна статистика таблицы.

Параметр `pretty` хорошо сочетается с `compact = 1`: он скрывает шаги `Expression` и подробную информацию о действиях, благодаря чему план становится удобнее для чтения.

Подробный пример с JOIN:

```sql
CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);

EXPLAIN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id FORMAT Raw;
```

```text
Output: id, value, id, value

Join (JOIN FillRightFirst)
│  t1[100] ⋈ t2[100]
│  Type: inner | Strictness: all | Algorithm: SpillingHashJoin(HashJoin)
│  Result rows: 100
│  Join conditions: id = id
│  Output:
│    Left:  id, value
│    Right: id, value
├──ReadFromMergeTree (default.t1)
│     Read type: Default
│     Parts: 1 | Granules: 1
│     Output: id, value
│     Runtime filters: RF1(id, id from default.t2)
└──BuildRuntimeFilter (Build runtime join filter on id)
   │  Filter id: RF1
   │  Source table: default.t2
   └──ReadFromMergeTree (default.t2)
         Read type: Default
         Parts: 1 | Granules: 1
         Output: id, value
```

<div id="explain-pipeline">
  ### EXPLAIN PIPELINE
</div>

Настройки:

* `header` — Выводит заголовок для каждого выходного порта. По умолчанию: 0.
* `graph` — Выводит граф, описанный на языке описания графов [DOT](https://en.wikipedia.org/wiki/DOT_\(graph_description_language\)). По умолчанию: 0.
* `compact` — Выводит граф в компактном режиме, если включена настройка `graph`. По умолчанию: 1.
* `compact_repeated_processor_chains` — Объединяет соседние повторяющиеся цепочки процессоров в текстовом выводе, показывая одну копию цепочки с количеством повторений. Это упрощает чтение параллельных конвейеров, когда одна и та же цепочка встречается много раз, например в операциях JOIN. На вывод графа это не влияет. По умолчанию: 0.

```text
Resize 16 → 1
  FillingRightJoinSide          │
    SimpleSquashingTransform    │ × 16
      Resize 1 → 16
```

Когда `compact=0` и `graph=1`, к именам процессоров будет добавлен дополнительный суффикс с уникальным идентификатором процессора.

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

<div id="explain-estimate">
  ### EXPLAIN ESTIMATE
</div>

Показывает оценочное количество строк, marks и частей, которые будут прочитаны из таблиц в ходе выполнения запроса. Работает с таблицами семейства [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree).

**Пример**

Создание таблицы:

```sql title="Query"
CREATE TABLE ttt (i Int64) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity = 16, write_final_mark = 0;
INSERT INTO ttt SELECT number FROM numbers(128);
OPTIMIZE TABLE ttt;
```

```sql title="Query"
EXPLAIN ESTIMATE SELECT * FROM ttt;
```

```text title="Response"
┌─database─┬─table─┬─parts─┬─rows─┬─marks─┐
│ default  │ ttt   │     1 │  128 │     8 │
└──────────┴───────┴───────┴──────┴───────┘
```

<div id="explain-whatif">
  ### EXPLAIN WHATIF
</div>

Оценивает, какую выгоду гипотетический индекс пропуска данных может дать для запроса `SELECT`, *без* материализации индекса на диске. Определите один или несколько кандидатов с помощью [`CREATE HYPOTHETICAL INDEX`](/ru/sql-reference/statements/hypothetical-index#create-hypothetical-index), затем выполните `EXPLAIN WHATIF SELECT ...`, чтобы для каждого кандидата увидеть: применимость, оценочное число прочитанных marks, оценочный объём в байтах и skip ratio.

**Синтаксис**

```sql
EXPLAIN WHATIF [empirical = 0] SELECT ...
```

**Настройки**

* `empirical` — `1` (по умолчанию) запускает индекс в памяти по гранулам, оставшимся после отсеивания по baseline, чтобы измерить долю пропуска (верхнюю границу). `0` пропускает этот этап. В любом случае, если `empirical` не дает результата (отключен или индекс нельзя вычислить в памяти), оценка переключается на [статистику](/ru/engines/table-engines/mergetree-family/mergetree#column-statistics) столбца, а если недоступно ни то ни другое — в итоге на сводку только по применимости.

**Вывод**

```text
Baseline (after PK + partition + existing indexes):
  table:       db.t
  parts:       1
  marks:       100
  est_bytes:   1.50 MiB             (only when the query reads rows)

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    15.00 KiB           (only when baseline bytes are known)
  skip_ratio:   99.0%

Estimation:
  source:           empirical | statistical | applicability_only
  empirical_status: ok | unsupported | disabled
  sampled_parts:    50 / 100        (only when source = empirical)
  sampled_marks:    50 / 100        (only when source = empirical)
  elapsed_us:       631             (only when source = empirical)
```

* `source` — как была получена оценка.
  * `empirical`: индекс строится в памяти по гранулам, оставшимся после baseline pruning, и подсчитывается, сколько гранул индекс мог бы пропустить. Это верхняя граница — см. ограничения в [`CREATE HYPOTHETICAL INDEX`](/ru/sql-reference/statements/hypothetical-index#limitations).
  * `statistical`: вычисляется на основе статистики столбцов. Используется, когда эмпирическая оценка отключена (`empirical = 0`) или не дала результата, и для соответствующих столбцов определена статистика.
  * `applicability_only`: индекс применим к предикату, но ни эмпирическая, ни статистическая оценка не дали результата (например, `empirical = 0` и статистика столбцов не определена). В качестве консервативной границы выводится `skip_ratio: 0.0%`.
* `sampled_parts` / `sampled_marks` — `<baseline-pruned> / <total in the table>`. Показывает, какая доля таблицы осталась после pruning по PK, партиции и существующим индексам, то есть входные данные для hypothetical index.
* `est_bytes` — оценка объёма читаемых байтов, вычисленная на основе среднего размера строки в таблице, поэтому она приблизительна и зависит от хранилища и сжатия. Строка baseline появляется только если запрос читает строки; строка для каждого кандидата — только если известна базовая оценка в байтах.

Параметр записывается inline между `WHATIF` и `SELECT` — ключевое слово `SETTINGS` отсутствует (это соответствует тому, как другие варианты `EXPLAIN` принимают свои параметры).

Если для таблицы не определены hypothetical indexes, `EXPLAIN WHATIF` возвращает `status: not_applicable` с подсказкой создать такой индекс.

**Объединённая строка (несколько кандидатов)**

Когда два или более кандидата оцениваются эмпирически, `EXPLAIN WHATIF` добавляет один дополнительный блок с именем `(combined: idx_a, idx_b, ...)` после строк отдельных кандидатов. Он показывает совокупную выгоду от одновременного наличия *всех* этих индексов: при реальном чтении гранула сохраняется только в том случае, если она проходит *каждый* индекс пропуска данных, поэтому объединённая оценка представляет собой пересечение гранул, оставшихся после применения кандидатов. Поэтому её `skip_ratio` как минимум не ниже, чем у лучшего отдельного кандидата: взаимодополняющие индексы вместе отсекают больше, а избыточные не меняют результат.

Учитываются только кандидаты с `source: empirical`, поскольку комбинированная строка строится путём пересечения их множеств выживших гранул. Кандидаты с оценкой `statistical` или `applicability_only` не имеют данных по отдельным гранулам и исключаются; поэтому комбинированный блок появляется только тогда, когда как минимум два кандидата дали эмпирическую оценку, а в остальных случаях не выводится (например, при `empirical = 0`). Значения его полей оценки совпадают с полями эмпирического блока отдельного кандидата, кроме `elapsed_us`, которое равно `0` — комбинированная оценка вычисляется на основе сканирований отдельных кандидатов, а не нового сканирования. Синтетическое имя `(combined: ...)` служит только меткой отчёта и не может использоваться с `force_data_skipping_indices`.

**Эмпирический пример**

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
```

Гипотетический `minmax` отсёк бы 99 из 100 меток, оставив только 1 — `skip_ratio: 99.0%`. (`est_bytes` — это оценка, основанная на среднем размере строки, поэтому точное значение может различаться.)

**Статистический пример**

Статистика [столбцов](/ru/engines/table-engines/mergetree-family/mergetree#column-statistics) по умолчанию отключена. Чтобы задействовать ветку `statistical`, сначала задайте её для соответствующих столбцов и дождитесь завершения мутации materialize:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;
```

Затем отключите эмпирический режим, чтобы оценщик снова использовал статистику столбцов:

```sql
EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

Число берётся из selectivity статистики столбца для `b < 10` (примерно 10 строк из 10000) и приводится как верхняя граница для `skip_ratio`. `sampled_parts` / `sampled_marks` отсутствуют — данные не читались.

Если недоступен ни один из путей (например, `empirical = 0` и статистика столбцов не определена), оценщик сообщает `source: applicability_only` и консервативное значение `skip_ratio: 0.0%`.

<div id="explain-table-override">
  ### EXPLAIN TABLE OVERRIDE
</div>

Показывает результат переопределения схемы таблицы для таблицы, к которой обращаются через табличную функцию.
Также выполняет некоторую проверку и генерирует исключение, если переопределение могло бы привести к какому-либо сбою.

**Пример**

Предположим, у вас есть удалённая таблица MySQL следующего вида:

```sql title="Query"
CREATE TABLE db.tbl (
    id INT PRIMARY KEY,
    created DATETIME DEFAULT now()
)
```

```sql title="Query"
EXPLAIN TABLE OVERRIDE mysql('127.0.0.1:3306', 'db', 'tbl', 'root', 'clickhouse')
PARTITION BY toYYYYMM(assumeNotNull(created))
```

```text title="Response"
┌─explain─────────────────────────────────────────────────┐
│ PARTITION BY uses columns: `created` Nullable(DateTime) │
└─────────────────────────────────────────────────────────┘
```

:::note
Проверка не является исчерпывающей, поэтому успешный запрос не гарантирует, что переопределение не приведёт к проблемам.
:::