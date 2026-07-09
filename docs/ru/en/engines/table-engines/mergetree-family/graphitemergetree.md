---
description: 'Предназначен для прореживания и агрегации/усреднения (rollup) данных Graphite.'
sidebar_label: 'GraphiteMergeTree'
sidebar_position: 90
slug: /engines/table-engines/mergetree-family/graphitemergetree
title: 'Движок таблицы GraphiteMergeTree'
doc_type: 'guide'
---

Этот движок предназначен для прореживания и агрегации/усреднения (rollup) данных [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Он может быть полезен разработчикам, которые хотят использовать ClickHouse в качестве хранилища данных для Graphite.

Для хранения данных Graphite можно использовать любой движок таблицы ClickHouse, если rollup не требуется. Но если rollup нужен, используйте `GraphiteMergeTree`. Этот движок сокращает объем хранимых данных и повышает эффективность запросов Graphite.

Этот движок наследует свойства [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

<div id="creating-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

См. подробное описание запроса [CREATE TABLE](/ru/sql-reference/statements/create/table).

Таблица для данных Graphite должна содержать следующие столбцы:

* Имя метрики (Graphite sensor). Тип данных: `String`.

* Время измерения метрики. Тип данных: `DateTime`.

* Значение метрики. Тип данных: `Float64`.

* Версия метрики. Тип данных: любой числовой тип (ClickHouse сохраняет строки с наибольшей версией или последнюю записанную строку, если версии совпадают. Остальные строки удаляются при слиянии частей данных).

Имена этих столбцов должны быть заданы в конфигурации rollup.

**Параметры GraphiteMergeTree**

* `config_section` — имя секции в файле конфигурации, где заданы правила rollup.

**Секции запроса**

При создании таблицы `GraphiteMergeTree` требуются те же [секции](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table), что и при создании таблицы `MergeTree`.

<details markdown="1">
  <summary>Устаревший метод создания таблицы</summary>

  :::note
  Не используйте этот метод в новых проектах и по возможности переведите старые проекты на метод, описанный выше.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      EventDate Date,
      Path String,
      Time DateTime,
      Value Float64,
      Version <Numeric_type>
      ...
  ) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
  ```

  Все параметры, кроме `config_section`, имеют то же значение, что и в `MergeTree`.

  * `config_section` — имя секции в файле конфигурации, где заданы правила rollup.
</details>

<div id="rollup-configuration">
  ## Конфигурация rollup
</div>

Настройки rollup задаются параметром [graphite&#95;rollup](../../../operations/server-configuration-parameters/settings.md#graphite) в конфигурации сервера. Имя параметра может быть любым. Можно создать несколько конфигураций и использовать их для разных таблиц.

Структура конфигурации rollup:

required-columns
patterns

<div id="required-columns">
  ### Обязательные столбцы
</div>

<div id="path_column_name">
  #### `path_column_name`
</div>

`path_column_name` — имя столбца, в котором хранится имя метрики (Graphite sensor). Значение по умолчанию: `Path`.

<div id="time_column_name">
  #### `time_column_name`
</div>

`time_column_name` — Название столбца, в котором хранится время измерения метрики. Значение по умолчанию: `Time`.

<div id="value_column_name">
  #### `value_column_name`
</div>

`value_column_name` — Имя столбца, хранящего значение метрики для момента времени, указанного в `time_column_name`. Значение по умолчанию: `Value`.

<div id="version_column_name">
  #### `version_column_name`
</div>

`version_column_name` — имя столбца, в котором хранится версия метрики. Значение по умолчанию: `Timestamp`.

<div id="patterns">
  ### Шаблоны
</div>

Структура раздела `patterns`:

```text
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

:::important
Шаблоны должны быть строго упорядочены:

1. Шаблоны без `function` или `retention`.
2. Шаблоны, содержащие и `function`, и `retention`.
3. Шаблон `default`.
   :::

При обработке строки ClickHouse проверяет правила в разделах `pattern`. Каждый раздел `pattern` (включая `default`) может содержать параметр `function` для агрегации, параметры `retention` или и то, и другое. Если имя метрики соответствует `regexp`, применяются правила из раздела (или разделов) `pattern`; в противном случае используются правила из раздела `default`.

Поля для разделов `pattern` и `default`:

* `rule_type` - тип правила. Оно применяется только к определённым метрикам. Движок использует его, чтобы разделять простые и тегированные метрики. Необязательный параметр. Значение по умолчанию: `all`.
  Он не нужен, если производительность не критична или используется только один тип метрик, например простые метрики. По умолчанию создаётся только один набор правил. В противном случае, если определён любой из специальных типов, создаются два разных набора. Один для простых метрик (root.branch.leaf) и один для тегированных метрик (root.branch.leaf;tag1=value1).
  Правила по умолчанию в итоге попадают в оба набора.
  Допустимые значения:
  * `all` (по умолчанию) - универсальное правило, используется, если `rule_type` не указан.
  * `plain` - правило для простых метрик. Поле `regexp` обрабатывается как regular expression.
  * `tagged` - правило для тегированных метрик (метрики хранятся в DB в формате `someName?tag1=value1&tag2=value2&tag3=value3`). Шаблон регулярного выражения должен быть отсортирован по именам тегов; первый тег должен быть `__name__`, если он существует. Поле `regexp` обрабатывается как regular expression.
  * `tag_list` - правило для тегированных метрик, простой DSL для более удобного описания метрики в формате graphite: `someName;tag1=value1;tag2=value2`, `someName` или `tag1=value1;tag2=value2`. Поле `regexp` преобразуется в правило `tagged`. Сортировка по именам тегов не требуется, это будет сделано автоматически. Значение тега (но не имя) может быть задано как regular expression, например `env=(dev|staging)`.
* `regexp` – шаблон регулярного выражения для имени метрики (обычный или DSL).
* `age` – Минимальный возраст данных в секундах.
* `precision`– Насколько точно определяется возраст данных в секундах. Должен быть делителем 86400 (числа секунд в сутках).
* `function` – Имя агрегирующей функции, применяемой к данным, возраст которых попадает в диапазон `[age, age + precision]`. Допустимые функции: min / max / any / avg. Среднее вычисляется неточно, как среднее от средних.

<div id="configuration-example">
  ### Пример конфигурации без указания типов правил
</div>

```xml
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

<div id="configuration-typed-example">
  ### Пример конфигурации с типами правил
</div>

```xml
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

:::note
rollup данных выполняется во время слияний. Обычно для старых партиций слияния не запускаются, поэтому для выполнения rollup необходимо принудительно запустить внеплановое слияние с помощью [optimize](../../../sql-reference/statements/optimize.md). Либо использовать дополнительные инструменты, например [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::