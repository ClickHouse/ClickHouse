---
alias: []
description: 'Формат ввода и вывода для документов GeoJSON FeatureCollection: при вводе — одна строка на объект со столбцами id, geometry и properties; при выводе — один объект на строку.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Данные [GeoJSON](https://geojson.org/) передаются в виде единого документа [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3), который ClickHouse сопоставляет с тремя столбцами — `id`, `geometry` и `properties` — по одному набору для каждого `Feature`. [Чтение](#reading-data) документа дает по одной строке на каждый объект, а [запись](#writing-data) — по одному объекту на каждую строку.

<div id="reading-data">
  ## Чтение данных
</div>

При чтении `FeatureCollection` создается по одной строке на каждый объект со следующей фиксированной схемой:

| Столбец      | Тип                | Описание                                                                                                                                                                             |
| ------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`         | `Nullable(String)` | Поле `id` объекта (JSON-строка или число), сохраненное как текст; `NULL`, если `id` отсутствует или равно `null`, при этом явно заданный пустой строковый `id` сохраняется как `''`. |
| `geometry`   | `Geometry`         | Геометрия объекта, сохраненная как тип варианта `Geometry`.                                                                                                                          |
| `properties` | `Nullable(JSON)`   | Объект `properties` объекта, сохраненный в полуструктурированном столбце `JSON`. Явно заданное `"properties": null` сохраняется как `NULL`.                                          |

Каждая геометрия сохраняется в типе `Geometry` ClickHouse (то есть в `Variant`). Поддерживаются следующие геометрические типы GeoJSON: `Point`, `LineString`, `MultiLineString`, `Polygon` и `MultiPolygon`. Два других геометрических типа GeoJSON, `GeometryCollection` и `MultiPoint`, не могут быть представлены типом `Geometry`; по умолчанию попытка прочитать один из них в столбец `geometry` вызывает исключение, но это поведение можно изменить так, чтобы вместо этого подставлялся `NULL` — см. [Обработка неподдерживаемых геометрических типов](#unsupported-geometry) ниже. По умолчанию столбец `geometry` содержит `NULL` только в том случае, если геометрия объекта — это явно заданный JSON `null`; при `input_format_geojson_unsupported_geometry_handling = 'null'` он также содержит `NULL` для неподдерживаемого геометрического типа.

Структура документа проверяется: верхнеуровневый `type` должен быть `FeatureCollection`, а каждый элемент `features` должен иметь `type` `Feature`. По умолчанию координаты должны удовлетворять ограничениям формы GeoJSON: `LineString` (и каждая линия в `MultiLineString`) должен содержать как минимум две точки, а кольцо `Polygon` (и каждое кольцо в `MultiPolygon`) должно быть замкнутым и содержать как минимум четыре точки (см. [Валидация геометрии](#geometry-validation)). Некорректные документы отклоняются, а не загружаются незаметно.

Порядок ключей может быть произвольным: верхнеуровневый `type` может находиться до или после массива `features`, а внутри объекта геометрии `coordinates` может находиться до или после `type`.

Вывод схемы возвращает приведенную выше фиксированную схему, поэтому `DESCRIBE` и `SELECT ... FROM format(...)` работают без определения таблицы.

Рассмотрим следующий GeoJSON‑файл `london.geojson`, содержащий смесь геометрических типов:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [-0.0761, 51.5081]},
            "properties": {"name": "Tower of London", "feature_type": "landmark", "year_built": 1078}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-0.2500, 51.4700], [-0.1800, 51.4900], [-0.1200, 51.5060], [-0.0700, 51.5050], [0.0000, 51.5100]]
            },
            "properties": {"name": "River Thames", "feature_type": "river", "length_km": 346}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-0.1880, 51.5074], [-0.1533, 51.5074], [-0.1533, 51.5153], [-0.1880, 51.5153], [-0.1880, 51.5074]]]
            },
            "properties": {"name": "Hyde Park", "feature_type": "park", "area_km2": 1.42}
        }
    ]
}
```

Мы можем сделать запрос к файлу и посмотреть геометрические типы:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name────────────┬─geo_type───┐
│ 1  │ Tower of London │ Point      │
│ 2  │ River Thames    │ LineString │
│ 3  │ Hyde Park       │ Polygon    │
└────┴─────────────────┴────────────┘
```

Расширение файла `.geojson` определяется автоматически, поэтому аргумент формата можно опустить:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

Мы можем использовать `variantType`, чтобы определить базовый тип каждого объекта Geometry:

```sql title="Query"
SELECT properties.name AS name, geometry, variantType(geometry)
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
geometry:              (-0.0761,51.5081)
variantType(geometry): Point

Row 2:
──────
name:                  River Thames
geometry:              [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
variantType(geometry): LineString

Row 3:
──────
name:                  Hyde Park
geometry:              [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
variantType(geometry): Polygon
```

И вот как можно извлечь исходные данные:

```sql title="Query"
SELECT properties.name AS name, variantType(geometry), geometry.Point, geometry.LineString, geometry.Polygon
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
variantType(geometry): Point
geometry.Point:        (-0.0761,51.5081)
geometry.LineString:   []
geometry.Polygon:      []

Row 2:
──────
name:                  River Thames
variantType(geometry): LineString
geometry.Point:        (0,0)
geometry.LineString:   [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
geometry.Polygon:      []

Row 3:
──────
name:                  Hyde Park
variantType(geometry): Polygon
geometry.Point:        (0,0)
geometry.LineString:   []
geometry.Polygon:      [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
```

При обращении к подстолбцу `Geometry` возвращается значение, если строка содержит этот тип, а в противном случае — значение типа по умолчанию: `(0,0)` для `Point` и `[]` для типов на основе массивов. Поэтому используйте `variantType(geometry)`, чтобы определить, какой из них задан.

Мы также можем загружать данные GeoJSON в таблицу:

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   Nullable(JSON),
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO london
SELECT id, geometry, properties
FROM file('london.geojson', GeoJSON);
```

Затем выполните запрос по типу объекта:

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM london
ORDER BY id;
```

```response title="Response"
┌─name────────────┬─feature_type─┬─geo_type───┐
│ Tower of London │ landmark     │ Point      │
│ River Thames    │ river        │ LineString │
│ Hyde Park       │ park         │ Polygon    │
└─────────────────┴──────────────┴────────────┘
```

Мы также можем автоматически определить схему GeoJSON-данных без определения таблицы:

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ Nullable(JSON)   │
└────────────┴──────────────────┘
```

<div id="unsupported-geometry">
  ### Обработка неподдерживаемых геометрических типов
</div>

Некоторые допустимые геометрические типы GeoJSON — такие как `GeometryCollection` и `MultiPoint` — не могут быть представлены типом `Geometry` в ClickHouse. Вы можете задать, что должно происходить, когда такую геометрию нужно сохранить в столбце `geometry`, с помощью настройки `input_format_geojson_unsupported_geometry_handling`. Возможные значения:

* `'throw'` — сгенерировать исключение (по умолчанию)
* `'null'` — вставить значение `NULL` в столбец `geometry` и продолжить парсинг

Эта обработка применяется только при чтении столбца `geometry`. Если `geometry` не входит в число запрошенных выходных столбцов (например, `SELECT id FROM ...`), неподдерживаемая геометрия всё равно проверяется на корректность структуры, но эта обработка не срабатывает — исключение не генерируется и `NULL` не вставляется, поскольку значение геометрии не материализуется.

<div id="reading-limitations">
  ### Ограничения
</div>

При чтении сохраняется только то, что соответствует фиксированной схеме, поэтому часть информации GeoJSON теряется:

* Возвращаются только `id`, `geometry` и `properties`; остальная структура документа не представляется в виде столбцов.
* Третья координата позиции (высота) и все последующие отбрасываются — позиции становятся `[longitude, latitude]`.
* `bbox` и сторонние элементы (например, `name` или `crs` верхнего уровня либо дополнительные элементы внутри `Feature`) игнорируются.
* Числовой `id` сохраняется как текст, поэтому различие между строкой и числом теряется; отсутствующий или `null` `id` становится `NULL`.
* `GeometryCollection` и `MultiPoint` не могут быть представлены — см. [Обработка неподдерживаемых геометрических типов](#unsupported-geometry).

<div id="writing-data">
  ## Запись данных
</div>

При записи результирующего набора создаётся один GeoJSON [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3), по одному `Feature` на каждую строку.

Столбцы результирующего набора сопоставляются каждому `Feature` следующим образом:

| Элемент `Feature` | Формируется из                             | Примечания                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------- | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`            | —                                          | Всегда `"Feature"`.                                                                                                                                                                                                                                                                                                                                                                           |
| `geometry`        | единственного столбца геометрического типа | Требуется ровно один столбец геометрического типа, иначе запрос отклоняется. Геометрия со значением `NULL` записывается как `null`.                                                                                                                                                                                                                                                           |
| `id`              | столбца с именем `id`                      | Опускается, если значение равно `NULL`. Столбец `String` записывается как JSON-строка, а числовой столбец — как JSON-число.                                                                                                                                                                                                                                                                   |
| `properties`      | всех остальных столбцов                    | Если есть единственный столбец с именем `properties` и его тип является объектоподобным (`JSON`, `Map` или именованный `Tuple`), он записывается напрямую как объект `properties`, а не вкладывается под ключ `properties`. В противном случае каждый оставшийся столбец становится отдельным свойством с ключом, равным имени столбца (если таких столбцов нет, записывается пустой объект). |

Столбец геометрического типа может иметь тип `Geometry` или конкретный геотип; каждый из них сопоставляется с типом геометрии GeoJSON:

| Тип ClickHouse    | GeoJSON `"type"`                    |
| ----------------- | ----------------------------------- |
| `Point`           | `Point`                             |
| `LineString`      | `LineString`                        |
| `MultiLineString` | `MultiLineString`                   |
| `Polygon`         | `Polygon`                           |
| `MultiPolygon`    | `MultiPolygon`                      |
| `Ring`            | `Polygon` (одно линейное кольцо)    |
| `Geometry`        | тип активного варианта (или `null`) |

`Ring` не является типом геометрии GeoJSON — [линейное кольцо](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) является компонентом `Polygon` — поэтому значение `Ring` записывается как `Polygon` с одним линейным кольцом.

<div id="writing-examples">
  ### Примеры
</div>

Продолжая работу с таблицей `london`, [созданной выше](#reading-data), при экспорте обычных столбцов атрибутов каждый столбец, кроме `id` и `geometry`, становится свойством:

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

Поскольку единственный столбец типа object с именем `properties` записывается напрямую, при чтении GeoJSON‑файла и последующей записи обратно документ воспроизводится в исходном виде (для файла автоматически определяются столбцы `id`, `geometry` и `properties`):

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

Числовой столбец `id` записывается как число в JSON (`Nullable` `id` со значением `NULL` полностью опускается):

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

`Ring` записывается как `Polygon` с одним кольцом:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### Запись в файл
</div>

Используйте `INTO OUTFILE`, чтобы записать GeoJSON‑файл на стороне клиента:

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

Сервер сам может записывать файл с помощью табличной функции `file` (расширение `.geojson` автоматически выбирает формат):

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="reading-limitations">
  ### Ограничения
</div>

:::note
Гео-типы ClickHouse не содержат информации о системе координат, поэтому при выводе предполагается, что координаты уже заданы в WGS84 как долгота/широта в порядке `[longitude, latitude]`, как того требует [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4). Перепроецирование и перестановка осей не выполняются, поэтому спроецированные координаты — или данные, сохранённые как `(latitude, longitude)` — дают структурно корректный, но не соответствующий стандарту GeoJSON.
:::

Вывод отражает только то, что хранится в ClickHouse:

* Информация, отброшенная при чтении, — высота точки, `bbox`, дополнительные поля и различие между строковым и числовым `id` — не может быть восстановлена; см. [Ограничения чтения](#reading-limitations).
* Координаты записываются из значений `Float64` с использованием их кратчайшего представления, допускающего корректный round-trip.
* Объект `properties`, взятый напрямую из столбца `JSON`, выводится в каноническом порядке ключей типа `JSON`, который может отличаться от исходного.

Геометрии записываются в точности в том виде, в каком они хранятся: порядок координат и направление обхода (winding) сохраняются. По умолчанию при записи проверяется корректность формы GeoJSON (см. [Валидация геометрии](#geometry-validation)): геометрия, не являющаяся корректной формой GeoJSON, например `LineString` с одной точкой или незамкнутое кольцо `Polygon`, отклоняется, чтобы записанный документ можно было затем прочитать обратно. Чтобы вместо этого выводить такие геометрии как есть и получать структурно корректный, но не соответствующий стандарту GeoJSON, установите `format_geojson_validate_geometry = 0`. Инвариант правила правой руки (winding) не проверяется ни в одном случае, а различие между `null` и пустым объектом `properties` сохраняется.

<div id="geometry-validation">
  ## Валидация геометрии
</div>

Параметр `format_geojson_validate_geometry` определяет, проверяет ли формат соблюдение правил структуры геометрии из [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1) в обоих направлениях. По умолчанию он включен.

Если параметр включен, геометрия, нарушающая правила структуры GeoJSON, отклоняется: `LineString` (или линия в `MultiLineString`) менее чем с двумя точками; кольцо `Polygon` или `MultiPolygon` менее чем с четырьмя точками либо с различающимися первой и последней точками (незамкнутое кольцо); а также пустой `MultiLineString`, `Polygon` или `MultiPolygon`. Те же правила действуют и при чтении такого документа, и при записи такого значения ClickHouse, поэтому записанный документ всегда можно прочитать обратно.

Если параметр отключен, эти правила структуры не применяются ни в одном направлении: вырожденные геометрии читаются как есть и записываются как есть. Это позволяет значениям геометрии ClickHouse, не являющимся корректными геометриями GeoJSON, проходить через этот формат с сохранением при записи и чтении, ценой создания документов, не являющихся корректным GeoJSON.

Проверка выполняется только на структурном уровне: она проверяет количество точек и замкнутость колец. Геометрическая корректность формы не проверяется, поэтому структурно корректная, но геометрически вырожденная геометрия принимается в обоих направлениях — например, полигон нулевой площади, самопересекающееся кольцо или полигон, у которого внутренние кольца находятся вне внешнего кольца. Ориентация обхода колец полигона по правилу правой руки (winding) также никогда не проверяется.

Одна проверка не зависит от этого параметра: нефинитные координаты (`NaN`, `Inf`) всегда отклоняются, поскольку их нельзя представить в виде чисел JSON.