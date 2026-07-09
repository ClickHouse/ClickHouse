---
description: 'Страница с подробным описанием анализатора запросов ClickHouse'
keywords: ['анализатор']
sidebar_label: 'Анализатор'
slug: /operations/analyzer
title: 'Анализатор'
doc_type: 'reference'
---

В версии ClickHouse `24.3` новый анализатор запросов был включен по умолчанию.
Подробнее о том, как он работает, можно прочитать [здесь](/ru/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

<div id="known-incompatibilities">
  ## Известные несовместимости
</div>

Хотя здесь исправлено множество ошибок и добавлены новые оптимизации, это также вносит некоторые обратно несовместимые изменения в поведение ClickHouse. Ознакомьтесь со следующими изменениями, чтобы понять, как переписать свои запросы для анализатора.

<div id="invalid-queries-are-no-longer-optimized">
  ### Недопустимые запросы больше не оптимизируются
</div>

В прежней инфраструктуре планирования запросов оптимизации на уровне AST применялись до этапа проверки запроса.
Оптимизации могли преобразовать исходный запрос так, что он становился корректным и исполняемым.

В анализаторе проверка запроса выполняется до этапа оптимизации.
Это означает, что недопустимые запросы, которые раньше можно было выполнить, теперь не поддерживаются.
В таких случаях запрос нужно исправить вручную.

<div id="example-1">
  #### Пример 1
</div>

Следующий запрос использует столбец `number` в списке проекции, хотя после агрегации доступен только `toString(number)`.
В старом анализаторе `GROUP BY toString(number)` оптимизировался до `GROUP BY number,`, из-за чего запрос считался допустимым.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### Пример 2
</div>

Та же проблема возникает и в этом запросе. Столбец `number` используется после агрегации с другим ключом.
Предыдущий анализатор запросов исправлял этот запрос, перенося фильтр `number > 5` из предложения `HAVING` в предложение `WHERE`.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

Чтобы исправить запрос, следует переместить все условия для неагрегированных столбцов в раздел `WHERE`, чтобы привести запрос в соответствие со стандартным синтаксисом SQL:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### `CREATE VIEW` с некорректным запросом
</div>

Анализатор всегда выполняет проверку типов.
Ранее можно было создать `VIEW` с некорректным запросом `SELECT`.
В этом случае ошибка возникала при первом `SELECT` или `INSERT` (для `MATERIALIZED VIEW`).

Теперь создать `VIEW` таким образом невозможно.

<div id="example-view">
  #### Пример
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### Известные несовместимости оператора `JOIN`
</div>

<div id="join-using-column-from-projection">
  #### `JOIN` с использованием столбца из проекции
</div>

Псевдоним из списка `SELECT` по умолчанию нельзя использовать в качестве ключа `JOIN USING`.

Новая настройка `analyzer_compatibility_join_using_top_level_identifier` при включении изменяет поведение `JOIN USING`: при разрешении идентификаторов приоритет отдается выражениям из списка проекции запроса `SELECT`, а не столбцам левой таблицы напрямую.

Например:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

Если для `analyzer_compatibility_join_using_top_level_identifier` задано значение `true`, условие JOIN интерпретируется как `t1.a + 1 = t2.b`, что соответствует поведению более ранних версий.
Результат будет `2, 'two'`.
Если значение настройки равно `false`, по умолчанию используется условие JOIN `t1.b = t2.b`, и запрос вернет `2, 'one'`.
Если `b` отсутствует в `t1`, запрос завершится ошибкой.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### Изменения в поведении при использовании `JOIN USING` и столбцов `ALIAS`/`MATERIALIZED`
</div>

В анализаторе использование `*` в запросе с `JOIN USING`, включающем столбцы `ALIAS` или `MATERIALIZED`, по умолчанию добавляет эти столбцы в набор результатов.

Например:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

В анализаторе результат этого запроса будет включать столбец `payload` вместе с `id` из обеих таблиц.
Напротив, предыдущий анализатор включал эти столбцы `ALIAS` только при включении определённых настроек (`asterisk_include_alias_columns` или `asterisk_include_materialized_columns`),
и столбцы могли отображаться в другом порядке.

Чтобы обеспечить согласованные и предсказуемые результаты, особенно при переносе старых запросов на анализатор, рекомендуется явно указывать столбцы в предложении `SELECT`, а не использовать `*`.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### Обработка модификаторов типов столбцов в предложении `USING`
</div>

В новой версии анализатора правила определения общего супертипа для столбцов, указанных в предложении `USING`, были унифицированы, чтобы результаты стали более предсказуемыми,
особенно при работе с такими модификаторами типов, как `LowCardinality` и `Nullable`.

* `LowCardinality(T)` и `T`: если столбец типа `LowCardinality(T)` участвует в JOIN со столбцом типа `T`, результирующим общим супертипом будет `T`, то есть модификатор `LowCardinality` фактически отбрасывается.
* `Nullable(T)` и `T`: если столбец типа `Nullable(T)` участвует в JOIN со столбцом типа `T`, результирующим общим супертипом будет `Nullable(T)`, что обеспечивает сохранение возможности принимать значение NULL.

Например:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

В этом запросе для `id` определяется общий супертип `String`, а модификатор `LowCardinality` из `t1` отбрасывается.

<div id="projection-column-names-changes">
  ### Изменения имён столбцов проекции
</div>

При вычислении имён проекций псевдонимы не подставляются.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### Несовместимые типы аргументов функции
</div>

В анализаторе вывод типов происходит на этапе анализа исходного запроса.
Это изменение означает, что проверка типов выполняется до укороченного вычисления; поэтому аргументы функции `if` всегда должны иметь общий супертип.

Например, следующий запрос завершается ошибкой `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### Неоднородные кластеры
</div>

Анализатор существенно меняет протокол взаимодействия между серверами в кластере. Поэтому выполнять распределённые запросы на серверах с разными значениями настройки `enable_analyzer` невозможно.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### Мутации обрабатываются предыдущим анализатором
</div>

Мутации по-прежнему используют старый анализатор.
Это означает, что некоторые новые возможности ClickHouse SQL нельзя использовать в мутациях. Например, оператор `QUALIFY`.
С текущим состоянием можно ознакомиться [здесь](https://github.com/ClickHouse/ClickHouse/issues/61563).

<div id="unsupported-features">
  ### Неподдерживаемые возможности
</div>

Ниже приведён список возможностей, которые анализатор пока не поддерживает:

* Индекс Annoy.
* Индекс Hypothesis. Работа над поддержкой ведётся [здесь](https://github.com/ClickHouse/ClickHouse/pull/48381).
* Оконное представление не поддерживается. Поддержка в будущем не планируется.

<div id="cloud-migration">
  ## Миграция в Cloud
</div>

Мы включаем новый анализатор запросов на всех экземплярах, где он сейчас отключен, чтобы обеспечить новые функциональные улучшения и оптимизации производительности. Это изменение вводит более строгие правила области видимости в SQL, поэтому клиентам потребуется вручную обновить запросы, которые им не соответствуют.

<div id="migration-workflow">
  ### Процесс миграции
</div>

1. Определите запрос, применив фильтр к `system.query_log` по `normalized_query_hash`:

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. Выполните запрос с включенным анализатором, добавив следующие настройки.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. Переработайте запрос и проверьте его результаты, чтобы убедиться, что они совпадают с выводом при отключенном анализаторе.

Ознакомьтесь с наиболее частыми несовместимостями, выявленными в ходе внутреннего тестирования.

<div id="unknown-expression-identifier">
  ### Неизвестный идентификатор выражения
</div>

Ошибка: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. Код исключения: 47

Причина: Запросы, использующие нестандартное устаревшее поведение с излишне свободной трактовкой правил — например, ссылки на вычисляемые псевдонимы в фильтрах, неоднозначные проекции подзапросов или «динамическую» область видимости CTE, — теперь корректно распознаются как недопустимые и сразу отклоняются.

Решение: Обновите шаблоны SQL следующим образом:

* Логика фильтрации: Перенесите условия из `WHERE` в `HAVING`, если фильтрация идёт по результатам, или продублируйте выражение в `WHERE`, если фильтрация идёт по исходным данным.
* Область видимости подзапроса: Явно выбирайте все столбцы, необходимые внешнему запросу.
* Ключи JOIN: Используйте `ON` с полными выражениями вместо `USING`, если ключ — это псевдоним.
* Во внешних запросах обращайтесь к псевдониму самого подзапроса/CTE, а не к таблицам внутри него.

<div id="non-aggregated-columns-in-group-by">
  ### Неагрегированные столбцы в GROUP BY
</div>

Ошибка: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. Код Исключения: 215

Причина: Старый анализатор позволял выбирать столбцы, которых нет в предложении GROUP BY (часто при этом бралось произвольное значение). Анализатор следует стандарту SQL: каждый выбранный столбец должен быть либо агрегатом, либо ключом группировки.

Решение: Оберните столбец в `any()`, `argMax()` или добавьте его в GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### Дублирующиеся имена CTE
</div>

Ошибка: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Код Исключения: 179

Причина: Старый анализатор позволял определять несколько общих табличных выражений (WITH ...) с одним и тем же именем, при этом более позднее перекрывало предыдущее. Анализатор запрещает такую неоднозначность.

Решение: Переименуйте повторяющиеся CTE так, чтобы их имена были уникальными.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### Неоднозначные идентификаторы столбцов
</div>

Ошибка: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Код исключения: 207

Причина: В запросе используется имя столбца, которое присутствует в нескольких таблицах в JOIN, без указания таблицы-источника. Старый анализатор часто определял столбец по внутренней логике, а новый анализатор требует явного имени.

Решение: Полностью указывайте столбец в формате table&#95;alias.column&#95;name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### Недопустимое использование FINAL
</div>

Ошибка: `Table expression modifiers FINAL are not supported for subquery...` или `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). Коды исключений: 1, 181

Причина: FINAL — это модификатор хранилища таблицы (в частности, [Shared]ReplacingMergeTree). Анализатор отклоняет FINAL, если он применяется к:

* Подзапросам или производным таблицам (например, FROM (SELECT ...) FINAL).
* Движкам таблиц, которые его не поддерживают (например, SharedMergeTree).

Решение: Применяйте FINAL только к исходной таблице внутри подзапроса или удалите его, если движок его не поддерживает.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### Регистронезависимость функции `countDistinct()`
</div>

Ошибка: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. Код Исключения: 46

Причина: Имена функций чувствительны к регистру либо строго сопоставляются в анализаторе. `countdistinct` (целиком в нижнем регистре) больше не распознаётся автоматически.

Решение: Используйте стандартную `countDistinct` (camelCase) или специфичную для ClickHouse функцию `uniq`.