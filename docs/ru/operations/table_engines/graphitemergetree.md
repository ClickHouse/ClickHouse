# GraphiteMergeTree

Движок предназначен для прореживания и агрегирования/усреднения (rollup) данных [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Он может быть интересен разработчикам, которые хотят использовать ClickHouse как хранилище данных для Graphite.

Если rollup не требуется, то для хранения данных Graphite можно использовать любой движок таблиц ClickHouse, в противном случае используйте `GraphiteMergeTree`. Движок уменьшает объем хранения и повышает эффективность запросов от Graphite.

Движок наследует свойства от [MergeTree](mergetree.md).

## Создание таблицы

```sql
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

Описание параметров запроса смотрите в [описании запроса](../../query_language/create.md).

Таблица для данных Graphite должна содержать следующие столбцы:

- Колонка с названием метрики (Graphite sensor). Тип данных: `String`.

- Столбец со временем измерения метрики. Тип данных `DateTime`.

- Столбец со значением метрики. Тип данных: любой числовой.

- Столбец с версией метрики. Тип данных: любой числовой.

    ClickHouse сохраняет строки с последней версией или последнюю записанную строку, если версии совпадают. Другие строки удаляются при слиянии кусков данных.

Имена этих столбцов должны быть заданы в конфигурации rollup.

**Параметры GraphiteMergeTree**

- `config_section` — имя раздела в конфигурационном файле, в котором находятся правила rollup.

**Секции запроса**

При создании таблицы `GraphiteMergeTree` используются те же [секции](mergetree.md#table_engine-mergetree-creating-a-table) запроса, что при создании таблицы `MergeTree`.

<details markdown="1"><summary>Устаревший способ создания таблицы</summary>

!!! attention
Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ описанный выше.

```sql
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

- `config_section` — имя раздела в конфигурационном файле, в котором находятся правила rollup.

</details>

## Конфигурация rollup

Настройки для прореживания данных задаются параметром [graphite_rollup](../server_settings/settings.md#server_settings-graphite_rollup). Имя параметра может быть любым. Можно создать несколько конфигураций и использовать их для разных таблиц.

Структура конфигурации rollup:

```
required-columns
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
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

**Важно**: порядок разделов `pattern` должен быть следующим:

1. Разделы *без* параметра `function` *или* `retention`.
1. Разделы *с* параметрами `function` *и* `retention`.
1. Раздел `default`.

При обработке строки ClickHouse проверяет правила в разделах `pattern`. Каждый из разделов `pattern` (включая `default`) может содержать параметр `function` для аггрегации, правила `retention` для прореживания или оба эти параметра. Если имя метрики соответствует шаблону `regexp`, то применяются правила из раздела (или разделов) `pattern`, в противном случае из раздела `default`.

Поля для разделов `pattern` и `default`:

- `regexp` – шаблон имени метрики.
- `age` – минимальный возраст данных в секундах.
- `precision` – точность определения возраста данных в секундах. Должен быть делителем для 86400 (количество секунд в дне).
- `function` – имя агрегирующей функции, которую следует применить к данным, чей возраст оказался в интервале `[age, age + precision]`.

`required-columns`:

- `path_column_name` — колонка с названием метрики (Graphite sensor).
- `time_column_name` — столбец со временем измерения метрики.
- `value_column_name` — столбец со значением метрики в момент времени, установленный в `time_column_name`.
- `version_column_name` — столбец с версией метрики.

Пример настройки:

```xml
<graphite_rollup>
    <path_column_name>Path</path_column_name>
    <time_column_name>Time</time_column_name>
    <value_column_name>Value</value_column_name>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>\.count$</regexp>
        <function>sum</function>
    </pattern>
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

[Оригинальная статья](https://clickhouse.yandex/docs/ru/operations/table_engines/graphitemergetree/) <!--hide-->
