<a name="table_engines-graphitemergetree"></a>

# GraphiteMergeTree

Движок предназначен для rollup (прореживания и агрегирования/усреднения) данных [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Он может быть интересен разработчикам, которые хотят использовать ClickHouse как хранилище данных для Graphite.

Graphite хранит в ClickHouse полные данные, а получать их может следующими способами:

-   Без прореживания.

    Используется движок [MergeTree](mergetree.md#table_engines-mergetree).

-   С прореживанием.

    Используется движок `GraphiteMergeTree`.

Движок наследует свойства MergeTree. Настройки прореживания данных задаются параметром [graphite_rollup](../server_settings/settings.md#server_settings-graphite_rollup) в конфигурации сервера .

## Использование движка

Таблица с данными Graphite должна содержать как минимум следующие поля:

-   `Path` - имя метрики (сенсора Graphite).
-   `Time` - время измерения.
-   `Value` - значение метрики в момент времени Time.
-   `Version` - настройка, которая определяет какое значение метрики с одинаковыми Path и Time останется в базе.

Шаблон правил rollup:

```text
pattern
    regexp
    function
    age -> precision
    ...
pattern
    ...
default
    function
       age -> precision
    ...
```

При обработке записи ClickHouse проверит правила в секции `pattern`. Если имя метрики соответствует шаблону `regexp`, то  применяются правила из `pattern`, в противном случае из `default`.

Поля шаблона правил.

- `age` - Минимальный возраст данных в секундах.
- `function` - Имя агрегирующей функции, которую следует применить к данным, чей возраст оказался в интервале `[age, age + precision]`.
- `precision` - Точность определения возраста данных в секундах.
- `regexp` - Шаблон имени метрики.

Пример настройки:

```xml
<graphite_rollup>
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
