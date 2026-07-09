---
description: 'Документация по типу данных JSON в ClickHouse со встроенной поддержкой
  работы с данными JSON'
keywords: ['json', 'тип данных']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'Тип данных JSON'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="Ознакомьтесь с нашим руководством по лучшим практикам для JSON: примеры, расширенные возможности и рекомендации по использованию типа JSON." icon="book" infoText="Подробнее" infoUrl="/docs/best-practices/use-json-where-appropriate" title="Ищете руководство?" />
</Link>

<br />

Тип `JSON` хранит документы JavaScript Object Notation (JSON) в одном столбце.

:::note
В ClickHouse с открытым исходным кодом тип данных JSON помечен как готовый к использованию в продакшн, начиная с версии 25.3. В более ранних версиях использовать этот тип в продакшн не рекомендуется.
:::

Чтобы объявить столбец типа `JSON`, можно использовать следующий синтаксис:

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```

Ниже приведены определения параметров из синтаксиса выше:

| Параметр                    | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Значение по умолчанию |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------- |
| `max_dynamic_paths`         | Необязательный параметр, указывающий, сколько путей может храниться отдельно в виде подстолбцов в рамках одного отдельно хранимого блока данных (например, в пределах одной части данных таблицы семейства MergeTree). <br /><br />Если этот лимит превышен, все остальные пути будут храниться вместе в единой структуре, называемой [общие данные](#shared-data-structure).<br /><br />Также есть [способы](#controlling-the-number-of-dynamic-paths) изменить лимит динамических путей без изменения этого параметра. | `1024`                |
| `max_dynamic_types`         | Необязательный параметр в диапазоне от `1` до `255`, указывающий, сколько различных типов данных может храниться отдельно в столбце одного пути с типом `Dynamic` в рамках одного отдельно хранимого блока данных (например, в пределах одной части данных таблицы семейства MergeTree). <br /><br />Если этот лимит превышен, все новые типы будут храниться вместе в единой структуре с именем `shared variant`.                                                                                                       | `32`                  |
| `some.path TypeName`        | Необязательная подсказка типа для конкретного пути в JSON. Такие пути всегда будут храниться как подстолбцы с указанным типом.                                                                                                                                                                                                                                                                                                                                                                                           |                       |
| `SKIP path.to.skip`         | Необязательная подсказка для конкретного пути, который нужно пропустить при разборе JSON. Такие пути никогда не будут храниться в JSON-столбце. Если указанный путь является вложенным объектом JSON, будет пропущен весь вложенный объект.                                                                                                                                                                                                                                                                              |                       |
| `SKIP REGEXP 'path_regexp'` | Необязательная подсказка с регулярным выражением, используемая для пропуска путей при разборе JSON. Все пути, соответствующие этому регулярному выражению, никогда не будут храниться в JSON-столбце.                                                                                                                                                                                                                                                                                                                    |                       |

<WhenToUseJson />

<div id="creating-json">
  ## Создание `JSON`
</div>

В этом разделе мы рассмотрим разные способы создания `JSON`.

<div id="using-json-in-a-table-column-definition">
  ### Использование `JSON` при определении столбца таблицы
</div>

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

<div id="using-cast-with-json">
  ### Использование CAST с `::JSON`
</div>

Различные типы можно приводить с помощью специального синтаксиса `::JSON`.

<div id="cast-from-string-to-json">
  #### Преобразование CAST из `String` в `JSON`
</div>

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-tuple-to-json">
  #### CAST из `Tuple` в `JSON`
</div>

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-map-to-json">
  #### CAST из `Map` в `JSON`
</div>

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
JSON-пути хранятся в уплощённом виде. Это означает, что, когда объект JSON формируется по пути вида `a.b.c`,
невозможно определить, должен ли объект быть построен как `{ "a.b.c" : ... }` или `{ "a": { "b": { "c": ... } } }`.
Наша реализация всегда исходит из второго варианта.

Например:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

вернёт:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

а **не**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## Чтение JSON-путей как подстолбцов
</div>

Тип `JSON` поддерживает чтение каждого пути как отдельного подстолбца.
Если тип запрошенного пути не указан в объявлении типа `JSON`,
то подстолбец этого пути всегда будет иметь тип [Dynamic](/ru/sql-reference/data-types/dynamic.md).

Например:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

Вы также можете использовать функцию `getSubcolumn`, чтобы считывать подстолбцы из значений типа JSON:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

Если запрошенный путь отсутствует в данных, для него будут подставлены значения `NULL`:

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

Давайте проверим типы данных возвращаемых подстолбцов:

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

Как видим, для `a.b` используется тип `UInt32`, как и было указано в объявлении типа JSON,
а для всех остальных подстолбцов используется тип `Dynamic`.

Подстолбцы типа `Dynamic` также можно читать с помощью специального синтаксиса `json.some.path.:TypeName`:

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

Подстолбцы `Dynamic` можно привести к любому типу данных. В этом случае будет сгенерировано исключение, если внутренний тип в `Dynamic` нельзя привести к запрошенному типу:

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
Для эффективного чтения подстолбцов из компактных частей MergeTree убедитесь, что включена настройка MergeTree [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts).
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## Чтение вложенных JSON-объектов как подстолбцов
</div>

Тип `JSON` поддерживает чтение вложенных объектов в виде подстолбцов типа `JSON` с использованием специального синтаксиса `json.^some.path`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
Когда пути хранятся в [общих данных](#shared-data-structure) базового типа (`map`), чтение подстолбцов подобъектов может быть неэффективным, поскольку требует сканирования всей общей структуры данных. При использовании сериализации общих данных `map_with_buckets` или `advanced` чтение подстолбцов из общих данных значительно оптимизировано.
:::

<div id="reading-json-combined-sub-columns">
  ## Чтение комбинированных подстолбцов JSON
</div>

Тип `JSON` поддерживает чтение пути как **комбинированного подстолбца** с использованием специального синтаксиса `json.@some.path`.
Комбинированный подстолбец для заданного пути возвращает:

* Литеральное значение, хранящееся по этому пути, как `Dynamic`, если по этому пути есть литеральное значение.
* Подобъект JSON по этому пути как `Dynamic`, если по этому пути нет литерального значения, но есть вложенные подпути.
* `NULL`, если для этого пути не существует ни литерального значения, ни каких-либо подпутей.

Это полезно, когда один и тот же путь в разных строках может содержать либо скалярное значение, либо вложенный объект, и удобнее, чем отдельно выполнять запрос к литеральному подстолбцу (`json.a`) и подстолбцу подобъекта (`json.^a`).

В следующем примере сравниваются все три типа подстолбцов для пути `a`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

* Row 1: `a` содержит литерал `42`. `json.a` возвращает его как `Dynamic(Int64)`, `json.^a` возвращает пустой подобъект `{}` (у `a` нет вложенных ключей), а `json.@a` возвращает литерал `42`.
* Row 2: `a` содержит вложенный объект. `json.a` возвращает `NULL` (по этому пути нет литерала), `json.^a` возвращает подобъект как `JSON`, а `json.@a` также возвращает подобъект как `Dynamic(JSON)`.
* Row 3: `a` полностью отсутствует. И `json.a`, и `json.@a` возвращают `NULL`, а `json.^a` возвращает пустой `{}`.

:::note
Если пути хранятся в базовых (`map`) [общих данных](#shared-data-structure), чтение комбинированных подстолбцов может быть неэффективным, так как требует сканирования всей общей структуры данных. При сериализации общих данных `map_with_buckets` или `advanced` чтение подстолбцов из общих данных существенно оптимизировано.
:::

<div id="type-inference-for-paths">
  ## Вывод типов для путей
</div>

При разборе `JSON` ClickHouse пытается определить наиболее подходящий тип данных для каждого JSON-пути.
Это работает так же, как и [автоматическое определение схемы](/ru/interfaces/schema-inference.md),
и управляется теми же настройками:

* [input&#95;format&#95;try&#95;infer&#95;dates](/ru/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/ru/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/ru/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ru/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/ru/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/ru/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/ru/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/ru/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/ru/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/ru/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

Рассмотрим несколько примеров:

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

<div id="handling-arrays-of-json-objects">
  ## Обработка массивов объектов JSON
</div>

JSON-пути, содержащие массив объектов, разбираются как тип `Array(JSON)` и записываются в столбец `Dynamic` для соответствующего пути.
Чтобы прочитать массив объектов, его можно извлечь из столбца `Dynamic` как подстолбец:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

Как вы, возможно, заметили, параметры `max_dynamic_types`/`max_dynamic_paths` у вложенного типа `JSON` были уменьшены по сравнению со значениями по умолчанию.
Это необходимо, чтобы количество подстолбцов во вложенных массивах объектов JSON не росло бесконтрольно.

Давайте попробуем прочитать подстолбцы из вложенного столбца `JSON`:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

Мы можем не указывать имена подстолбцов `Array(JSON)`, используя специальный синтаксис:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

Количество `[]` после path указывает на уровень массива. Например, `json.path[][]` будет преобразован в `json.path.:Array(Array(JSON))`

Давайте проверим пути и types внутри нашего `Array(JSON)`:

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

Прочитаем подстолбцы из столбца типа `Array(JSON)`:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

Мы также можем читать из вложенного столбца `JSON` подстолбцы подобъектов:

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

<div id="handling-json-keys-with-nulls">
  ## Обработка ключей JSON с NULL
</div>

В нашей реализации JSON `null` и отсутствие значения считаются равнозначными:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

Это означает, что невозможно определить, содержали ли исходные JSON-данные какой-либо путь со значением NULL или не содержали его вовсе.

<div id="handling-json-keys-with-dots">
  ## Обработка JSON-ключей с точками
</div>

Внутри JSON-столбца все пути и значения хранятся в уплощённом виде. Это означает, что по умолчанию эти 2 объекта считаются одинаковыми:

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

Оба варианта будут внутренне храниться как пара: путь `a.b` и значение `42`. При форматировании JSON мы всегда формируем вложенные объекты на основе частей пути, разделённых точками:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

Как видите, исходный JSON `{"a.b" : 42}` теперь преобразуется в `{"a" : {"b" : 42}}`.

Это ограничение также приводит к ошибке при разборе корректных объектов JSON, например такого:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

Если вы хотите сохранить ключи с точками и не представлять их как вложенные объекты, вы можете включить
настройку [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/ru/operations/settings/formats#json_type_escape_dots_in_keys) (доступна начиная с версии `25.8`). В этом случае при разборе все точки в ключах JSON будут
экранироваться как `%2E`, а при форматировании — преобразовываться обратно.

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

Чтобы прочитать ключ с экранированной точкой как подстолбец, нужно использовать экранированную точку в имени подстолбца:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

Примечание: из-за ограничений парсера и анализатора идентификаторов подстолбец `` json.`a.b` `` эквивалентен подстолбцу `json.a.b` и не сможет прочитать путь с экранированной точкой:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

Кроме того, если вы хотите указать подсказку для JSON-пути, содержащего ключи с точками (или использовать его в разделах `SKIP`/`SKIP REGEX`), в подсказке необходимо использовать экранированные точки:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

<div id="reading-json-type-from-data">
  ## Чтение типа JSON из данных
</div>

Все текстовые форматы
([`JSONEachRow`](/ru/interfaces/formats/JSONEachRow),
[`TSV`](/ru/interfaces/formats/TabSeparated),
[`CSV`](/ru/interfaces/formats/CSV),
[`CustomSeparated`](/ru/interfaces/formats/CustomSeparated),
[`Values`](/ru/interfaces/formats/Values) и т. д.) поддерживают чтение типа `JSON`.

Примеры:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

Для текстовых форматов, таких как `CSV`/`TSV`/и т. д., `JSON` разбирается из строки, содержащей объект JSON:

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

<div id="reaching-the-limit-of-dynamic-paths-inside-json">
  ## Достижение предела динамических путей в JSON
</div>

Тип данных `JSON` может хранить лишь ограниченное число путей как отдельные подстолбцы.
По умолчанию этот предел равен `1024`, но его можно изменить в объявлении типа с помощью параметра `max_dynamic_paths`.

Когда предел достигнут, все новые пути, вставляемые в столбец `JSON`, будут храниться в единой общей структуре данных.
Читать такие пути как подстолбцы по-прежнему можно,
но это может быть менее эффективно ([см. раздел об общих данных](#shared-data-structure)).
Этот предел нужен, чтобы избежать появления огромного числа различных подстолбцов, из-за которого таблица может стать непригодной для использования.

Посмотрим, что происходит при достижении этого предела в нескольких разных сценариях.

<div id="reaching-the-limit-during-data-parsing">
  ### Достижение предела при разборе данных
</div>

При разборе объектов `JSON` из данных, когда для текущего блока данных достигается предел,
все новые пути будут сохраняться в общей структуре данных. Мы можем использовать следующие две функции интроспекции: `JSONDynamicPaths`, `JSONSharedDataPaths`

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

Как видно, после вставки путей `e` и `f.g` был достигнут лимит,
и они были вставлены в общую структуру данных.

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### При слиянии частей данных в движках таблиц MergeTree
</div>

При слиянии нескольких частей данных в таблице `MergeTree` столбец `JSON` в результирующей части данных может достичь предела динамических путей
и не сможет хранить все пути из исходных частей в виде подстолбцов.
В этом случае ClickHouse выбирает, какие пути останутся подстолбцами после слияния, а какие будут храниться в общей структуре данных.
В большинстве случаев ClickHouse старается сохранить пути, содержащие
наибольшее количество значений, отличных от `NULL`, а более редкие пути переместить в общую структуру данных. Однако это зависит от реализации.

Рассмотрим пример такого слияния.
Сначала создадим таблицу со столбцом `JSON`, установим предел динамических путей равным `3`, а затем выполним вставку значений с `5` разными путями:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

Каждая вставка создаст отдельную часть данных со столбцом `JSON`, содержащим один путь:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

Теперь давайте объединим все части в одну и посмотрим, что произойдёт:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

Как видно, ClickHouse сохранил наиболее часто встречающиеся пути `a`, `b` и `c`, а пути `d` и `e` переместил в общую структуру данных.

<div id="shared-data-structure">
  ## Общая структура данных
</div>

Как было описано в предыдущем разделе, когда достигается предел `max_dynamic_paths`, все новые пути сохраняются в одной общей структуре данных.
В этом разделе мы подробно рассмотрим общую структуру данных и то, как из неё читаются подстолбцы путей.

См. раздел [&quot;функции интроспекции&quot;](/ru/sql-reference/data-types/newjson#introspection-functions), где подробнее описаны функции для проверки содержимого JSON-столбца.

<div id="shared-data-structure-in-memory">
  ### Общая структура данных в памяти
</div>

В памяти общая структура данных — это просто подстолбец типа `Map(String, String)`, в котором хранится сопоставление уплощённого JSON-пути со значением, закодированным в двоичном виде.
Чтобы извлечь из него подстолбец пути, мы просто перебираем все строки в этом столбце `Map` и пытаемся найти нужный путь и его значения.

<div id="shared-data-structure-in-merge-tree-parts">
  ### Общая структура данных в частях MergeTree
</div>

В таблицах [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) данные хранятся в частях данных, где всё размещается на диске (локальном или удалённом). При этом данные на диске могут храниться не так, как в памяти.
В настоящее время в частях данных MergeTree используются 3 разных варианта сериализации общей структуры данных: `map`, `map_with_buckets`
и `advanced`.

Версия сериализации задаётся настройками MergeTree
[object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
и [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
(часть нулевого уровня — это часть, создаваемая при вставке данных в таблицу; при слиянии части получают более высокий уровень).

Примечание: изменение сериализации общей структуры данных поддерживается только
для `v3` [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version)

<div id="shared-data-map">
  #### Map
</div>

В версии сериализации `map` общие данные сериализуются в виде одного столбца типа `Map(String, String)` — так же, как они хранятся в
памяти. Чтобы прочитать подстолбец пути из этого типа сериализации, ClickHouse считывает весь столбец `Map` и
извлекает запрошенный путь в памяти.

Эта сериализация эффективна для записи данных и чтения всего `JSON`-столбца, но неэффективна для чтения подстолбцов путей.

<div id="shared-data-map-with-buckets">
  #### Map с бакетами
</div>

В версии сериализации `map_with_buckets` общие данные сериализуются в виде `N` столбцов (&quot;бакетов&quot;) с типом `Map(String, String)`.
Каждый такой бакет содержит только подмножество путей. Чтобы прочитать подстолбец по пути из этого типа сериализации, ClickHouse
считывает весь столбец `Map` из одного бакета и извлекает нужный путь в памяти.

Эта сериализация менее эффективна для записи данных и чтения всего `JSON`-столбца, но более эффективна для чтения подстолбцов путей,
поскольку считывает данные только из нужных бакетов.

Количество бакетов `N` задаётся настройками MergeTree [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (8 по умолчанию)
и [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (32 по умолчанию).
Максимально допустимое значение для обеих настроек — 256.

<div id="shared-data-advanced">
  #### Продвинутый
</div>

В версии сериализации `advanced` общие данные сериализуются в специальную структуру данных, которая обеспечивает максимальную производительность
при чтении подстолбцов по путям за счёт хранения дополнительной информации, позволяющей считывать только данные по запрошенным путям.
Эта сериализация также поддерживает бакеты, поэтому каждый бакет содержит только подмножество путей.

Эта сериализация довольно неэффективна для записи данных (поэтому её не рекомендуется использовать для частей нулевого уровня); чтение всего столбца `JSON` немного менее эффективно по сравнению с сериализацией `map`, но для чтения подстолбцов по путям она очень эффективна.

Примечание: из-за хранения дополнительной информации внутри структуры данных объём данных на диске при использовании этой сериализации больше по сравнению с
сериализациями `map` и `map_with_buckets`.

Более подробный обзор новых сериализаций общих данных и подробности реализации приведены в [посте в блоге](https://clickhouse.com/blog/json-data-type-gets-even-better).

<div id="controlling-the-number-of-dynamic-paths">
  ## Управление количеством динамических путей в JSON в частях MergeTree
</div>

Основной способ задать ограничение на количество динамических путей в JSON — использовать параметр `max_dynamic_paths` в объявлении типа JSON.
Однако изменение `max_dynamic_paths` для существующих столбцов требует выполнения `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`, что запускает фоновую мутацию, переписывающую все существующие части.
Такая мутация может быть очень ресурсоемкой и влиять на производительность сервера до ее завершения. Чтобы этого избежать, можно использовать следующие 3 настройки, которые позволяют изменить ограничение на количество динамических путей в таблицах MergeTree для новых частей данных:

* `merge_max_dynamic_subcolumns_in_wide_part` - настройка MergeTree, которая ограничивает количество динамических подстолбцов для каждого JSON-столбца при слиянии в часть данных Wide.
* `merge_max_dynamic_subcolumns_in_compact_part` - настройка MergeTree, которая ограничивает количество динамических подстолбцов для каждого JSON-столбца при слиянии в часть данных Compact.
* `max_dynamic_subcolumns_in_json_type_parsing` - настройка сеанса, которая ограничивает количество динамических подстолбцов для каждого JSON-столбца при разборе JSON-данных в JSON-столбец.

Примечание: ограничение на количество динамических путей не может превышать значение, указанное в параметре `max_dynamic_paths`, даже если значения описанных настроек больше.

<div id="introspection-functions">
  ## Функции интроспекции
</div>

Есть несколько функций, которые помогают изучить содержимое JSON-столбца:

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**Примеры**

Давайте изучим содержимое набора данных [GH Archive](https://www.gharchive.org/) за `2020-01-01`:

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

<div id="alter-modify-column-to-json-type">
  ## ALTER MODIFY COLUMN в тип JSON
</div>

Можно изменить существующую таблицу, заменив тип столбца на новый тип `JSON`. На данный момент поддерживается только `ALTER` для столбцов типа `String`.

**Пример**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

<div id="lazy-type-hints">
  ## Ленивые подсказки типов (экспериментальная возможность)
</div>

:::note
Эта возможность является экспериментальной и требует включения настройки `allow_experimental_json_lazy_type_hints`.
:::

Когда вы добавляете или изменяете подсказки типов в JSON-столбце с помощью `ALTER TABLE ... MODIFY COLUMN`, ClickHouse обычно переписывает все части данных, чтобы материализовать новые подсказки типов. Для таблиц с большими объёмами исторических данных (сотни терабайт) это может быть чрезвычайно затратно.

**Ленивые подсказки типов** позволяют добавлять подсказки типов как операцию только с метаданными, без переписывания существующих данных:

* **Старые части**: подсказки типов применяются во время выполнения запроса через приведение из `Dynamic` к типу, указанному в подсказке
* **Новые части**: подсказки типов материализуются при операциях `INSERT`
* **Слияния**: подсказки типов материализуются при слиянии частей

Это означает, что вы можете добавлять подсказки типов мгновенно, а данные будут постепенно преобразовываться по мере выполнения обычных фоновых слияний.

<div id="enabling-lazy-type-hints">
  ### Включение ленивых подсказок типов
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### Пример
</div>

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

<div id="verifying-no-mutation-occurred">
  ### Проверка отсутствия мутации
</div>

Вы можете убедиться, что `ALTER` завершился без мутации, проверив таблицу `system.mutations`:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

При включенных ленивых подсказках типов этот запрос не возвращает ни одной строки, что подтверждает, что операция была выполнена только на уровне метаданных.

<div id="materializing-type-hints">
  ### Материализация подсказок типа
</div>

Чтобы материализовать подсказки типов в существующих данных, можно:

1. **Дождаться фоновых слияний**: ClickHouse автоматически материализует подсказки типов при слиянии частей
2. **Принудительно запустить слияние**: используйте `OPTIMIZE TABLE test_lazy FINAL`, чтобы немедленно слить все части
3. **Перезаписать части**: используйте `ALTER TABLE test_lazy REWRITE PARTS`, чтобы перезаписать части с новыми метаданными

<div id="lazy-type-hints-limitations">
  ### Ограничения
</div>

* Эта возможность является экспериментальной и может измениться в будущих версиях
* Преобразование типов при выполнении запроса может сопровождаться существенными накладными расходами по сравнению с заранее материализованными типами, особенно для крупных объектов JSON
* Эта возможность применяется только при изменении `typed_paths` (подсказок типов); другие параметры JSON, такие как `max_dynamic_paths`, `SKIP` или `SKIP REGEXP`, по-прежнему требуют мутаций

<div id="comparison-between-values-of-the-json-type">
  ## Сравнение значений типа JSON
</div>

Объекты JSON сравниваются так же, как значения типа Map.

Например:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**Примечание:** если 2 пути содержат значения разных типов данных, они сравниваются согласно [правилу сравнения](/ru/sql-reference/data-types/variant#comparing-values-of-variant-data) для типа данных `Variant`.

<div id="data-skipping-indexes-for-json">
  ## Индексы пропуска данных для JSON
</div>

[Индексы пропуска данных](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) можно использовать со столбцами `JSON` тремя способами:

1. **Индексы для конкретных подстолбцов** — создайте стандартный индекс пропуска данных для известного JSON-пути, как для обычного столбца. При этом индексируются *значения* по этому пути.
2. **Индексы по путям с `JSONAllPaths`** — индексируйте *набор путей*, присутствующих в каждой грануле, чтобы пропускать гранулы, которые заведомо не могут содержать запрашиваемый путь.
3. **Индексы по значениям с `JSONAllValues`** — индексируйте *все значения* по всем JSON-путям с помощью [текстового индекса](/ru/engines/table-engines/mergetree-family/textindexes.md), чтобы ускорить полнотекстовый поиск по любому подстолбцу JSON с помощью одного индекса.

<div id="json-indexes-on-subcolumns">
  ### Индексы для отдельных подстолбцов
</div>

Вы можете создать индекс пропуска данных для любого подстолбца JSON, используя тот же синтаксис, что и для обычных столбцов.
Поддерживается любой [тип индекса](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) (`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1` и т. д.).

Есть два способа обратиться к подстолбцу JSON в выражении индекса:

* **Типизированный путь**, заданный в подсказке типа JSON, — прямой доступ по имени: `json.a`.
* **Динамический путь** с явным приведением типа — используйте синтаксис приведения `::`: `json.b::String`.

Вы также можете использовать выражения, объединяющие несколько подстолбцов, например `json.a || json.b::String`.

<div id="json-indexes-on-subcolumns-example">
  #### Пример
</div>

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

Индекс `minmax` для типизированного подстолбца `data.sensor_id` ограничивает сканирование соответствующими гранулами:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

Индекс `bloom_filter` на подстолбце `data.location::String` с приведением типа тоже работает:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  ### Индексы по путям с JSONAllPaths
</div>

[Индексы пропуска данных](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) также можно создавать для столбцов `JSON` с помощью функции [`JSONAllPaths`](/ru/sql-reference/functions/json-functions#JSONAllPaths).
Это работает так же, как и создание индексов пропуска данных для столбцов [`Map`](/ru/sql-reference/data-types/map) через `mapKeys`: индекс хранит набор JSON-путей, присутствующих в каждой грануле, и использует его, чтобы пропускать гранулы, в которых не может содержаться запрашиваемый путь.

<div id="json-indexes-jsonallpaths-supported-types">
  #### Поддерживаемые типы индексов
</div>

`JSONAllPaths` можно использовать со следующими типами индексов пропуска данных:

* [`bloom_filter`](/ru/engines/table-engines/mergetree-family/mergetree#bloom-filter) — поддерживает `equals`, `in` и `IS NOT NULL`.
* [`tokenbf_v1`](/ru/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — поддерживает `equals` и `IS NOT NULL`.
* [`ngrambf_v1`](/ru/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — поддерживает `equals` и `IS NOT NULL`.
* [`text`](/ru/engines/table-engines/mergetree-family/textindexes) (обратный индекс) — поддерживает `equals`, `in` и `IS NOT NULL`.

<div id="json-indexes-on-subcolumns-example">
  #### Пример
</div>

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

Вы можете использовать `EXPLAIN indexes = 1`, чтобы убедиться, что индекс пропуска данных действительно используется. Если путь существует только в одной части, индекс пропускает другую часть:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

Если путь отсутствует во всех частях, пропускаются все части и гранулы:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` также использует индекс — он пропускает гранулы, в которых путь отсутствует (поскольку в этом случае значение равно `NULL`):

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallpaths-how-it-works">
  #### Как это работает
</div>

Выражение `JSONAllPaths(json_column)` формирует `Array(String)`, содержащий все пути, присутствующие в значении JSON.
Индекс пропуска данных сохраняет эти строки путей в своей структуре данных (bloom filter или обратный индекс).
Когда запрос фильтрует по `json.some.path`, индекс проверяет, присутствует ли строка `"some.path"` в индексе для каждой гранулы, и пропускает гранулы, где она отсутствует.

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### Безопасность при отсутствии путей
</div>

Когда JSON-путь отсутствует в грануле, подстолбец принимает значение:

* `NULL` для типа `Dynamic` (например, `json.path`) и подстолбцов типа `Nullable` (например, `json.path.:Int64`) — сравнения с `NULL` всегда возвращают false, поэтому пропуск безопасен.
* Значение этого типа по умолчанию для выражений `CAST` без `Nullable` (например, `json.path::Int64` даёт `0`, если путь отсутствует) — пропуск безопасен только тогда, когда сравниваемое значение отличается от значения по умолчанию. Индекс автоматически учитывает это различие.

<div id="json-indexes-jsonallvalues">
  ### Полнотекстовый поиск с JSONAllValues
</div>

[Текстовые индексы](/ru/engines/table-engines/mergetree-family/textindexes.md) можно использовать для ускорения полнотекстового поиска по JSON-столбцам с помощью функции [`JSONAllValues`](/ru/sql-reference/functions/json-functions#JSONAllValues).
`JSONAllValues` возвращает все значения из JSON-столбца в виде `Array(String)`, который можно индексировать текстовым индексом.
Один индекс на `JSONAllValues(json_column)` охватывает все JSON-пути, обеспечивая полнотекстовый поиск по любому подстолбцу без создания отдельных индексов для каждого пути.

Подробности и примеры см. в разделе [Индексы на основе значений с JSONAllValues](/ru/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) в документации по текстовым индексам.

<div id="tips-for-better-usage-of-the-json-type">
  ## Советы по более эффективному использованию типа JSON
</div>

Перед созданием столбца `JSON` и загрузкой в него данных обратите внимание на следующие рекомендации:

* Изучите свои данные и укажите как можно больше подсказок для путей с типами. Это сделает хранение и чтение данных гораздо эффективнее.
* Подумайте, какие пути вам понадобятся, а какие не понадобятся никогда. Укажите пути, которые вам не понадобятся, в разделе `SKIP`, а при необходимости — и в разделе `SKIP REGEXP`. Это повысит эффективность хранения.
* Не задавайте слишком большое значение параметру `max_dynamic_paths`, так как это может снизить эффективность хранения и чтения.
  Хотя это сильно зависит от параметров системы, таких как память, CPU и т. д., в качестве общего практического правила можно рекомендовать не задавать `max_dynamic_paths` больше 10 000 для хранилища на локальной файловой системе и 1024 для хранилища на удалённой файловой системе.

<div id="further-reading">
  ## Дополнительные материалы
</div>

* [Как мы создали новый мощный тип данных JSON для ClickHouse](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [JSON-челлендж на миллиард документов: ClickHouse против MongoDB, Elasticsearch и не только](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)