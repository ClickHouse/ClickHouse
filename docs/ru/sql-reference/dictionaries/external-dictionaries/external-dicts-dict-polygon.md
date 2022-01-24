---
toc_priority: 46
toc_title: Cловари полигонов
---

# Cловари полигонов {#polygon-dictionaries}

Словари полигонов позволяют эффективно искать полигон, в который попадают данные точки, среди множества полигонов.
Для примера: определение района города по географическим координатам.

Пример конфигурации словаря полигонов:

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

    <layout>
        <polygon>
            <store_polygon_key_column>1</store_polygon_key_column>
        </polygon>
    </layout>

    ...
</dictionary>
```

Соответствущий [DDL-запрос](../../../sql-reference/statements/create/dictionary.md#create-dictionary-query):
``` sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
...
```

При конфигурации словаря полигонов ключ должен иметь один из двух типов:

-   Простой полигон. Представляет из себя массив точек.
-   Мультиполигон. Представляет из себя массив полигонов. Каждый полигон задается двумерным массивом точек — первый элемент этого массива задает внешнюю границу полигона,
последующие элементы могут задавать дырки, вырезаемые из него.

Точки могут задаваться массивом или кортежем из своих координат. В текущей реализации поддерживается только двумерные точки.

Пользователь может [загружать свои собственные данные](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) во всех поддерживаемых ClickHouse форматах.

Доступно 3 типа [хранения данных в памяти](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md):

-   `POLYGON_SIMPLE`. Это наивная реализация, в которой на каждый запрос делается линейный проход по всем полигонам, и для каждого проверяется принадлежность без использования дополнительных индексов.

-   `POLYGON_INDEX_EACH`. Для каждого полигона строится отдельный индекс, который позволяет быстро проверять принадлежность в большинстве случаев (оптимизирован под географические регионы).
Также на рассматриваемую область накладывается сетка, которая значительно сужает количество рассматриваемых полигонов.
Сетка строится рекурсивным делением ячейки на 16 равных частей и конфигурируется двумя параметрами.
Деление прекращается при достижении глубины рекурсии `MAX_DEPTH` или в тот момент, когда ячейку пересекают не более `MIN_INTERSECTIONS` полигонов.
Для ответа на запрос находится соответствующая ячейка, и происходит поочередное обращение к индексу для сохранных в ней полигонов.

-   `POLYGON_INDEX_CELL`. В этом размещении также строится сетка, описанная выше. Доступны такие же параметры. Для каждой ячейки-листа строится индекс на всех попадающих в неё кусках полигонов, который позволяет быстро отвечать на запрос.

-   `POLYGON`. Синоним к `POLYGON_INDEX_CELL`.

Запросы к словарю осуществляются с помощью стандартных [функций](../../../sql-reference/functions/ext-dict-functions.md) для работы со внешними словарями.
Важным отличием является то, что здесь ключами будут являются точки, для которых хочется найти содержащий их полигон.

**Пример**

Пример работы со словарем, определенным выше:
``` sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

В результате исполнения последней команды для каждой точки в таблице `points` будет найден полигон минимальной площади, содержащий данную точку, и выведены запрошенные аттрибуты.

**Пример**

Вы можете читать столбцы из полигональных словарей с помощью SELECT, для этого включите `store_polygon_key_column = 1` в конфигурации словаря или соответствующего DDL-запроса.

Запрос:

``` sql
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = TinyLog;

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

Результат:

``` text
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```

