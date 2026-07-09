---
description: 'Документация по конструкции WITH'
sidebar_label: 'WITH'
slug: /sql-reference/statements/select/with
title: 'Конструкция WITH'
doc_type: 'reference'
---

ClickHouse поддерживает общие табличные выражения ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), общие скалярные выражения и рекурсивные запросы.

<div id="common-table-expressions">
  ## Общие табличные выражения
</div>

Общие табличные выражения представляют собой именованные подзапросы.
На них можно ссылаться по имени в любой части `SELECT`-запроса, где допускается табличное выражение.
На именованные подзапросы можно ссылаться по имени в области видимости текущего запроса или в областях видимости дочерних подзапросов.

Каждая ссылка на общее табличное выражение в `SELECT`-запросах всегда заменяется подзапросом из его определения, если CTE явно не определено как материализованное (см. [Материализованные общие табличные выражения](#materialized-common-table-expressions)).
Рекурсия предотвращается за счёт исключения текущего CTE из процесса разрешения идентификаторов.

Обратите внимание, что CTE не гарантируют одинаковые результаты во всех местах использования, поскольку запрос будет выполняться заново для каждого случая использования.

<div id="common-table-expressions-syntax">
  ### Синтаксис
</div>

```sql
WITH <identifier> AS [MATERIALIZED] <subquery expression>
```

<div id="common-table-expressions-example">
  ### Пример
</div>

Пример, когда подзапрос выполняется повторно:

```sql
WITH cte_numbers AS
(
    SELECT
        num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT
    count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers)
```

Если бы CTE передавали именно результаты, а не просто фрагмент кода, вы бы всегда видели `1000000`

Однако, поскольку мы дважды ссылаемся на `cte_numbers`, каждый раз генерируются случайные числа, и, соответственно, мы видим разные случайные результаты: `280501, 392454, 261636, 196227` и так далее...

<div id="materialized-common-table-expressions">
  ## Материализованные общие табличные выражения
</div>

По умолчанию ClickHouse подставляет подзапрос CTE в каждом месте, где на него есть ссылка, и каждый раз выполняет его заново.
Добавление ключевого слова `MATERIALIZED` указывает ClickHouse выполнить подзапрос CTE **ровно один раз**, сохранить результат во временной таблице и использовать эту таблицу для всех обращений к CTE.
Это особенно полезно, когда один и тот же CTE используется в запросе несколько раз (например, в self-join или в нескольких подзапросах `IN`), поскольку вычисление выполняется только один раз.

:::note
Материализованные CTE — **экспериментальная** возможность.
Для их использования должны быть включены [анализатор](/ru/operations/analyzer) и настройка `enable_materialized_cte`.
:::

<div id="common-table-expressions-syntax">
  ### Синтаксис
</div>

```sql
WITH <identifier> AS MATERIALIZED (<subquery>)
SELECT ...
```

<div id="materialized-cte-when-to-use">
  ### Когда использовать
</div>

Материализованные CTE особенно полезны, когда:

* Один и тот же CTE используется в запросе **более одного раза**.
  Без `MATERIALIZED` каждое обращение заново и независимо выполняет подзапрос.
* CTE содержит **недетерминированные** функции, такие как `generateRandom`.
  Материализация гарантирует, что во всех обращениях будут использоваться одни и те же данные.
* CTE включает **ресурсоемкие вычисления** (агрегации, JOIN, сканирование больших объемов данных), которые не следует повторять.

:::tip
Если к материализованному CTE обращаются только один раз, ClickHouse автоматически разворачивает его обратно в обычный подзапрос, чтобы избежать лишних накладных расходов.
:::

<div id="materialized-common-table-expressions-examples">
  ### Примеры
</div>

**Пример 1:** Самосоединение на материализованном CTE

Без `MATERIALIZED` обе стороны JOIN будут независимо выполнять подзапрос.
С `MATERIALIZED` таблица сканируется один раз, и обе стороны JOIN читают из одной и той же временной таблицы.

```sql
SET enable_materialized_cte = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE = Memory;
INSERT INTO users VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

WITH
    a AS MATERIALIZED (SELECT * FROM users WHERE name = 'Alice')
SELECT count() FROM a AS l JOIN a AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       1 │
└─────────┘
```

**Пример 2:** Детерминированные результаты с недетерминированными функциями

Обычные CTE с `generateRandom` дают разные результаты при каждом обращении.
Материализация CTE обеспечивает согласованность:

```sql
SET enable_materialized_cte = 1;

WITH cte_numbers AS MATERIALIZED
(
    SELECT num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers);
```

Поскольку оба обращения используют одни и те же материализованные данные, результат всегда равен `1000000`.

**Пример 3:** Цепочка материализованных CTE

Материализованные CTE могут ссылаться на другие материализованные CTE.
ClickHouse определяет зависимости и материализует их в правильном порядке:

```sql
SET enable_materialized_cte = 1;

WITH
    a AS MATERIALIZED (SELECT uid, name FROM users),
    b AS MATERIALIZED (SELECT uid FROM a)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

Порядок определений CTE не важен — допускаются ссылки на последующие определения:

```sql
SET enable_materialized_cte = 1;

WITH
    b AS MATERIALIZED (SELECT uid FROM a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

<div id="materialized-cte-restrictions">
  ### Ограничения
</div>

* **Требуется экспериментальная настройка**: настройка `enable_materialized_cte` должна быть включена.
* **Требуется анализатор**: материализованные CTE работают только при включенном [анализаторе](/ru/operations/analyzer) (`enable_analyzer = 1`).
* **Не поддерживается с `RECURSIVE`**: сочетание ключевых слов `MATERIALIZED` и `RECURSIVE` не допускается и приводит к исключению `UNSUPPORTED_METHOD`.
* **Коррелированные CTE запрещены**: материализованная CTE не может ссылаться на столбцы из внешних областей запроса.

<div id="common-scalar-expressions">
  ## Общие скалярные выражения
</div>

ClickHouse позволяет объявлять псевдонимы для произвольных скалярных выражений в конструкции `WITH`.
На общие скалярные выражения можно ссылаться из любого места запроса.

:::note
Если общее скалярное выражение ссылается на что-либо, кроме константного литерала, это может привести к появлению [свободных переменных](https://en.wikipedia.org/wiki/Free_variables_and_bound_variables).
ClickHouse разрешает любой идентификатор в ближайшей возможной области видимости, поэтому при конфликтах имен свободные переменные могут ссылаться на неожиданные сущности или приводить к коррелированному подзапросу.
Чтобы сделать поведение при разрешении иденении идентификаторов выражений более предсказуемым, рекомендуется определять CSE как [лямбда-функцию](/ru/sql-reference/functions/overview#arrow-operator-and-lambda) (это возможно только при включенном [анализаторе](/ru/operations/analyzer)), связывая все используемые идентификаторы.
:::

<div id="common-table-expressions-syntax">
  ### Синтаксис
</div>

```sql
WITH <expression> AS <identifier>
```

<div id="materialized-common-table-expressions-examples">
  ### Примеры
</div>

**Пример 1:** Использование константного выражения как &quot;переменной&quot;

```sql
WITH '2019-08-01 15:23:00' AS ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**Пример 2:** Использование функций высшего порядка для ограничения идентификаторов

```sql
WITH
    '.txt' as extension,
    (id, extension) -> concat(lower(id), extension) AS gen_name
SELECT gen_name('test', '.sql') as file_name;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Пример 3:** Использование функций высшего порядка со свободными переменными

Следующие запросы в этом примере показывают, что несвязанные идентификаторы разрешаются как сущность из ближайшей области видимости.
Здесь `extension` не связан в теле лямбда-функции `gen_name`.
Хотя `extension` задан как `'.txt'` в виде общего скалярного выражения в области видимости, где определяется и используется `generated_names`, он разрешается в столбец таблицы `extension_list`, поскольку доступен в подзапросе `generated_names`.

```sql
CREATE TABLE extension_list
(
    extension String
)
ORDER BY extension
AS SELECT '.sql';

WITH
    '.txt' as extension,
    generated_names as (
        WITH
            (id) -> concat(lower(id), extension) AS gen_name
        SELECT gen_name('test') as file_name FROM extension_list
    )
SELECT file_name FROM generated_names;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**Пример 4:** Исключение результата выражения sum(bytes) из списка столбцов в предложении SELECT

```sql
WITH sum(bytes) AS s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**Пример 5:** Использование результатов скалярного подзапроса

```sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10;
```

**Пример 6:** Повторное использование выражения в подзапросе

```sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

<div id="recursive-queries">
  ## Рекурсивные запросы
</div>

Необязательный модификатор `RECURSIVE` позволяет запросу в `WITH` ссылаться на собственный результат. Пример:

**Пример:** Суммирование целых чисел от 1 до 100

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

:::note
Рекурсивные CTE опираются на [анализатор запросов](/ru/operations/analyzer), добавленный в версии **`24.3`**. Если вы используете версию **`24.3+`** и сталкиваетесь с исключением **`(UNKNOWN_TABLE)`** или **`(UNSUPPORTED_METHOD)`**, это означает, что анализатор отключён для вашего экземпляра, роли или профиля. Чтобы активировать анализатор, включите настройку **`allow_experimental_analyzer`** или обновите настройку **`compatibility`** до более новой версии.
Начиная с версии `24.8` анализатор был полностью переведён в продакшн, а настройка `allow_experimental_analyzer` была переименована в `enable_analyzer`.
:::

Общая форма рекурсивного запроса `WITH` всегда состоит из нерекурсивной части, затем `UNION ALL`, затем рекурсивной части, причём только рекурсивная часть может содержать ссылку на собственный результат запроса. Рекурсивный CTE-запрос выполняется следующим образом:

1. Вычислите нерекурсивную часть. Поместите результат запроса нерекурсивной части во временную рабочую таблицу.
2. Пока рабочая таблица не пуста, повторяйте следующие шаги:
   1. Вычислите рекурсивную часть, подставляя текущее содержимое рабочей таблицы вместо рекурсивной самоссылки. Поместите результат запроса рекурсивной части во временную промежуточную таблицу.
   2. Замените содержимое рабочей таблицы содержимым промежуточной таблицы, затем очистите промежуточную таблицу.

Рекурсивные запросы обычно используются для работы с иерархическими или древовидными данными. Например, можно написать запрос, выполняющий обход дерева:

**Пример:** Обход дерева

Сначала создадим таблицу дерева:

```sql
DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    parent_id Nullable(UInt64),
    data String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');
```

Это дерево можно обойти с помощью такого запроса:

**Пример:** Обход дерева

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree;
```

```text
┌─id─┬─parent_id─┬─data──────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │
│  1 │         0 │ Child_1   │
│  2 │         0 │ Child_2   │
│  3 │         1 │ Child_1_1 │
└────┴───────────┴───────────┘
```

<div id="search-order">
  ### Порядок обхода
</div>

Чтобы получить порядок обхода в глубину, для каждой строки результата мы вычисляем массив строк, которые уже посетили:

**Пример:** Обход дерева в глубину

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY path;
```

```text
┌─id─┬─parent_id─┬─data──────┬─path────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │ [0]     │
│  1 │         0 │ Child_1   │ [0,1]   │
│  3 │         1 │ Child_1_1 │ [0,1,3] │
│  2 │         0 │ Child_2   │ [0,2]   │
└────┴───────────┴───────────┴─────────┘
```

Чтобы получить порядок обхода в ширину, обычно добавляют столбец, отслеживающий глубину поиска:

**Пример:** Обход дерева в ширину

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path, toUInt64(0) AS depth
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id]), depth + 1
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY depth;
```

```text
┌─id─┬─link─┬─data──────┬─path────┬─depth─┐
│  0 │ ᴺᵁᴸᴸ │ ROOT      │ [0]     │     0 │
│  1 │    0 │ Child_1   │ [0,1]   │     1 │
│  2 │    0 │ Child_2   │ [0,2]   │     1 │
│  3 │    1 │ Child_1_1 │ [0,1,3] │     2 │
└────┴──────┴───────────┴─────────┴───────┘
```

<div id="cycle-detection">
  ### Обнаружение циклов
</div>

Сначала создадим таблицу graph:

```sql
DROP TABLE IF EXISTS graph;
CREATE TABLE graph
(
    from UInt64,
    to UInt64,
    label String
) ENGINE = MergeTree ORDER BY (from, to);

INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');
```

Мы можем обойти этот граф с помощью следующего запроса:

**Пример:** Обход графа без проверки на циклы

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
    UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┐
│    1 │  4 │ 1 -> 4 │
│    1 │  2 │ 1 -> 2 │
│    1 │  3 │ 1 -> 3 │
│    2 │  3 │ 2 -> 3 │
│    4 │  5 │ 4 -> 5 │
└──────┴────┴────────┘
```

Но если мы добавим цикл в этот граф, предыдущий запрос завершится ошибкой `Maximum recursive CTE evaluation depth`:

```sql
INSERT INTO graph VALUES (5, 1, '5 -> 1');

WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
Code: 306. DB::Exception: Received from localhost:9000. DB::Exception: Maximum recursive CTE evaluation depth (1000) exceeded, during evaluation of search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to). Consider raising max_recursive_cte_evaluation_depth setting.: While executing RecursiveCTESource. (TOO_DEEP_RECURSION)
```

Стандартный способ обработки циклов — сформировать массив уже посещённых узлов:

**Пример:** Обход графа с обнаружением циклов

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label, false AS is_cycle, [tuple(g.from, g.to)] AS path FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label, has(path, tuple(g.from, g.to)), arrayConcat(sg.path, [tuple(g.from, g.to)])
    FROM graph g, search_graph sg
    WHERE g.from = sg.to AND NOT is_cycle
)
SELECT * FROM search_graph WHERE is_cycle ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┬─is_cycle─┬─path──────────────────────┐
│    1 │  4 │ 1 -> 4 │ true     │ [(1,4),(4,5),(5,1),(1,4)] │
│    4 │  5 │ 4 -> 5 │ true     │ [(4,5),(5,1),(1,4),(4,5)] │
│    5 │  1 │ 5 -> 1 │ true     │ [(5,1),(1,4),(4,5),(5,1)] │
└──────┴────┴────────┴──────────┴───────────────────────────┘
```

<div id="infinite-queries">
  ### Бесконечные запросы
</div>

Также можно использовать бесконечные рекурсивные CTE-запросы, если во внешнем запросе задан `LIMIT`:

**Пример:** Бесконечный рекурсивный CTE-запрос

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table
)
SELECT sum(number) FROM (SELECT number FROM test_table LIMIT 100);
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

<div id="trailing-comma">
  ## Запятая в конце
</div>

После последнего элемента в конструкции `WITH` допускается запятая:

```sql
WITH
    (SELECT sum(number) FROM numbers(10)) AS total,
    total * 2 AS doubled,
SELECT total, doubled;
```