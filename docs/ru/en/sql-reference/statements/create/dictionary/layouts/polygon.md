---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Полигональные словари'
sidebar_label: 'Polygon'
sidebar_position: 12
description: 'Настройка полигональных словарей для проверки вхождения точки в полигон.'
doc_type: 'справочник'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Словарь `polygon` (`POLYGON`) оптимизирован для запросов на определение вхождения точки в полигон, то есть для поиска по принципу &quot;обратного геокодирования&quot;.
Для заданной координаты (широты/долготы) он эффективно определяет, какой полигон или регион (из множества полигонов, например границ стран или регионов) содержит эту точку.
Он хорошо подходит для сопоставления координат местоположения с регионом, в который они входят.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="Polygon Dictionaries in ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

Пример настройки полигонального словаря:

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Configuration file">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
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
  </TabItem>
</Tabs>

<br />

При настройке полигонального словаря ключ должен иметь один из двух типов:

* Простой полигон. Это массив точек.
* MultiPolygon. Это массив полигонов. Каждый полигон представляет собой двумерный массив точек. Первый элемент этого массива — внешняя граница полигона, а последующие элементы задают области, которые должны быть из него исключены.

Точки можно задавать как массивом, так и кортежем координат. В текущей реализации поддерживаются только двумерные точки.

Пользователь может загружать собственные данные во всех форматах, поддерживаемых ClickHouse.

Доступны 3 типа [хранилища в оперативной памяти](./#storing-dictionaries-in-memory):

| Структура            | Описание                                                                                                                                                                                                                                                                                                                                                                           |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | Наивная реализация. Для каждого запроса выполняется линейный проход по всем полигонам с проверкой вхождения без дополнительных индексов.                                                                                                                                                                                                                                           |
| `POLYGON_INDEX_EACH` | Для каждого полигона строится отдельный индекс, что в большинстве случаев позволяет быстро проверять вхождение (оптимизировано для географических регионов). На область накладывается сетка, которая рекурсивно делит ячейки на 16 равных частей. Деление прекращается, когда глубина рекурсии достигает `MAX_DEPTH` или ячейка пересекает не более `MIN_INTERSECTIONS` полигонов. |
| `POLYGON_INDEX_CELL` | Также создаёт описанную выше сетку с теми же параметрами. Для каждой листовой ячейки строится индекс по всем частям полигонов, попадающим в неё, что позволяет быстро получать ответы на запросы.                                                                                                                                                                                  |
| `POLYGON`            | Синоним `POLYGON_INDEX_CELL`.                                                                                                                                                                                                                                                                                                                                                      |

Запросы к словарю выполняются с использованием стандартных [функций](/ru/sql-reference/functions/ext-dict-functions.md) для работы со словарями.
Важное отличие здесь в том, что ключами будут точки, для которых нужно найти содержащий их полигон.

**Пример**

Пример работы со словарём, определённым выше:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

В результате выполнения последней команды для каждой точки из таблицы &#39;points&#39; будет найден полигон минимальной площади, содержащий эту точку, а затем будут выведены запрошенные атрибуты.

**Пример**

Столбцы из полигональных словарей можно читать с помощью запроса SELECT — достаточно включить `store_polygon_key_column = 1` в конфигурации словаря или в соответствующем DDL-запросе.

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

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

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```