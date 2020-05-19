# Cловари полигонов {#slovari-polygonov}

ClickHouse поддерживает словари полигонов. Данный словарь является одной из реализаций внешних словарей, у которого задана специфичная структура. Общий вид кофигурации выглядит следующим образом:

``` xml
<dictionary>
    <structure>
        <key>
            <name>key</name>
            <type>Array(Array(Array(Array(Float64))))</type>
        </key>

        <attribute>
            <name>name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

        <attribute>
            <name>value</name>
            <type>UInt64</type>
            <null_value>0</null_value>
        </attribute>

    </structure>
</dictionary>
```

Соответствущий [DDL-запрос](../../../sql-reference/statements/create.md#create-dictionary-query):
``` sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
...
```

Ключом словаря является сам полигон, что довольно необычно. Все атрибуты являются опциональными, при желании туда можно добавить более подробную информацию. К примеру, может быть добавлен атрибут, содержащий информацию о числе людей, проживающих в данном регионе.

Для данного словаря доступно несколько типов [хранения в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md), сейчас доступно 4 типа:

-   POLYGON

-   GRID_POLYGON

-   BUCKET_POLYGON

-   ONE_BUCKET_POLYGOM

Тип хранения можно выбрать при создании словаря, указав его в соответствующем параметре `LAYOUT`. Пример `DDL-запроса`:

``` sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
LAYOUT(POLYGON());
...
```

Пользователь может [загружать свои собственные данные](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), представленные во всех поддерживаемых ClickHouse форматах.


Дальнейшие запросы к словарю осуществляются с помощью [dictGet](../../../sql-reference/functions/ext-dict-functions.md/#dictget), где ключами являются заданные точки. Один из возможных решений может быть использование таблицы ClickHouse:

Пользователь инициализирует таблицу с интересующими его точками. Это можно сделать следующим `DDL-запросом`:

``` sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
```

В качестве источника данных таблицы можно указать все поддерживаемые ClickHouse форматы данных, к примеру можно передать в таблицу файл с точками в формате `TSV`.

После этого, для выполнения запросов к словарю можно использовать следующую команду:

``` sql
SELECT 'dictGet', 'polygon_dict_name' AS dict_name, tuple(x, y) AS key,
       dictGet(dict_name, 'name', key),
       dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;

```

В результате ее исполнения для каждой точки в таблице `points` будет найден полигон минимальной площади, содержащий данную точку. Если точка не лежит ни в каком из полигонов, будут возвращены значения по умолчанию. 