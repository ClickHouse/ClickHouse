---
sidebar_position: 38
sidebar_label: GraphiteMergeTree
---

# GraphiteMergeTree {#graphitemergetree}

Движок предназначен для прореживания и агрегирования/усреднения (rollup) данных [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Он может быть интересен разработчикам, которые хотят использовать ClickHouse как хранилище данных для Graphite.

Если rollup не требуется, то для хранения данных Graphite можно использовать любой движок таблиц ClickHouse, в противном случае используйте `GraphiteMergeTree`. Движок уменьшает объём хранения и повышает эффективность запросов от Graphite.

Движок наследует свойства от [MergeTree](mergetree.md).

## Создание таблицы {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Смотрите описание запроса [CREATE TABLE](../../../engines/table-engines/mergetree-family/graphitemergetree.md#create-table-query).

В таблице должны быть столбцы для следующих данных:

-   Название метрики (сенсора Graphite). Тип данных: `String`.

-   Время измерения метрики. Тип данных `DateTime`.

-   Значение метрики. Тип данных: любой числовой.

-   Версия метрики. Тип данных: любой числовой (ClickHouse сохраняет строки с последней версией или последнюю записанную строку, если версии совпадают. Другие строки удаляются при слиянии кусков данных).

Имена этих столбцов должны быть заданы в конфигурации rollup.

**Параметры GraphiteMergeTree**

-   `config_section` — имя раздела в конфигурационном файле, в котором находятся правила rollup.

**Секции запроса**

При создании таблицы `GraphiteMergeTree` используются те же [секции](mergetree.md#table_engine-mergetree-creating-a-table) запроса, что и при создании таблицы `MergeTree`.

<details markdown="1">

<summary>Устаревший способ создания таблицы</summary>

    :::note "Attention"
    Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ описанный выше.
    :::
``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

Все параметры, кроме `config_section` имеют то же значение, что в `MergeTree`.

-   `config_section` — имя раздела в конфигурационном файле, в котором находятся правила rollup.

</details>

## Конфигурация Rollup {#rollup-configuration}

Настройки прореживания данных задаются параметром [graphite_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) в конфигурации сервера . Имя параметра может быть любым. Можно создать несколько конфигураций и использовать их для разных таблиц.

Структура конфигурации rollup:

``` text
required-columns
patterns
```

### Требуемые столбцы (required-columns) {#required-columns}

-   `path_column_name` — столбец, в котором хранится название метрики (сенсор Graphite). Значение по умолчанию: `Path`.
-   `time_column_name` — столбец, в котором хранится время измерения метрики. Значение по умолчанию: `Time`.
-   `value_column_name` — столбец со значением метрики в момент времени, установленный в `time_column_name`. Значение по умолчанию: `Value`.
-   `version_column_name` — столбец, в котором хранится версия метрики. Значение по умолчанию: `Timestamp`.

### Правила (patterns) {#patterns}

Структура раздела `patterns`:

``` text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

:::danger "Внимание"
    Правила должны быть строго упорядочены:

      1. Правила без `function` или `retention`.
      1. Правила одновремено содержащие `function` и `retention`.
      1. Правило `default`.

При обработке строки ClickHouse проверяет правила в разделе `pattern`. Каждый `pattern` (включая `default`) может содержать параметр агрегации `function`, параметр `retention`, или оба параметра одновременно. Если имя метрики соответствует шаблону `regexp`, то применяются правила `pattern`, в противном случае правило `default`.

Поля для разделов `pattern` и `default`:

-   `rule_type` - тип правила (применяется только к метрикам указанных типов), используется для разделения правил проверки плоских/теггированных метрик. Опциональное поле. Значение по умолчанию: `all`.  
Если используются метрики только одного типа или производительность проверки правил некритична, можно не использовать. По умолчанию создается только один тип правил для проверки. Иначе, если хотя бы для одного правила указано отличное от умолчания значение, создаются 2 независимых типа правил - для обычных (классические root.branch.leaf) и теггированных метрик (root.branch.leaf;tag1=value1).  
Правила по умолчанию попадают в оба правила обоих типов.  
Возможные значения:
    -   `all` (default) - универсальное правило, назначается также по умолчанию, если поле не задано
    -   `plain` - правило для плоских метрик (без тегов). Поле `regexp` обрабатывается как регулярное выражение.
    -   `tagged` - правило для теггированных метрик (метрика хранится в БД в формате `someName?tag1=value1&tag2=value2&tag3=value3`), регулярное выражение должно быть отсортированно по именам тегов, первым - значение тега `__name__`, если есть.  Поле `regexp` обрабатывается как регулярное выражение.
    -   `tag_list` - правило для теггированных метрик, простой DSL для упрощения задания регулярного выражения в формате тегов graphite `someName;tag1=value1;tag2=value2`, `someName` или `tag1=value1;tag2=value2`. Поле `regexp` транслируется в правило `tagged`. Cортировать по именам тегов не обязательно, оно отсортируется автоматически. Значение тега (но не имя) может быть регулярным выражением (например `env=(dev|staging)`).
-   `regexp` – шаблон имени метрики (регулярное выражение или DSL).
-   `age` – минимальный возраст данных в секундах.
-   `precision` – точность определения возраста данных в секундах. Должен быть делителем для 86400 (количество секунд в сутках).
-   `function` – имя агрегирующей функции, которую следует применить к данным, чей возраст оказался в интервале `[age, age + precision]`. Допустимые функции: min/max/any/avg. Avg вычисляется неточно, как среднее от средних.

### Пример конфигурации без разделения типа правил {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

### Пример конфигурации c разделением типа правил {#configuration-typed-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```


:::danger "Внимание"
    Прореживание данных производится во время слияний. Обычно для старых партиций слияния не запускаются, поэтому для прореживания надо инициировать незапланированное слияние используя [optimize](../../../sql-reference/statements/optimize.md). Или использовать дополнительные инструменты, например [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
