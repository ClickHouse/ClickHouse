---
alias: []
description: 'Formato de entrada y salida para documentos GeoJSON FeatureCollection: en la entrada, una fila por entidad con las columnas id, geometry y properties; en la salida, una entidad por fila.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Los datos [GeoJSON](https://geojson.org/) se intercambian como un único documento [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3), que ClickHouse asigna a tres columnas — `id`, `geometry` y `properties` —, un conjunto por cada `Feature`. [Leer](#reading-data) un documento produce una fila por `Feature`; [escribir](#writing-data) produce un `Feature` por fila.

<div id="reading-data">
  ## Lectura de datos
</div>

Leer una `FeatureCollection` produce una fila por entidad con el siguiente esquema fijo:

| Columna      | Tipo               | Descripción                                                                                                                                                                                               |
| ------------ | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | El miembro `id` de la entidad (una cadena o un número JSON), almacenado como texto; `NULL` si el `id` no está presente o es `null`, mientras que un `id` explícito de cadena vacía se conserva como `''`. |
| `geometry`   | `Geometry`         | La geometría de la entidad, almacenada como un tipo variante `Geometry`.                                                                                                                                  |
| `properties` | `Nullable(JSON)`   | El objeto `properties` de la entidad, almacenado como una columna `JSON` semiestructurada. Un `"properties": null` explícito se conserva como `NULL`.                                                     |

Cada geometría se almacena en el tipo `Geometry` de ClickHouse (un `Variant`). Los tipos de geometría GeoJSON admitidos son `Point`, `LineString`, `MultiLineString`, `Polygon` y `MultiPolygon`. Los otros dos tipos de geometría GeoJSON, `GeometryCollection` y `MultiPoint`, no pueden representarse con el tipo `Geometry`; leer uno de ellos en la columna `geometry` genera una excepción de forma predeterminada, aunque esto puede cambiarse para insertar `NULL` en su lugar; consulta [Manejo de tipos de geometría no admitidos](#unsupported-geometry) más abajo. De forma predeterminada, la columna `geometry` es `NULL` solo cuando la geometría de una entidad es un `null` JSON explícito; con `input_format_geojson_unsupported_geometry_handling = 'null'` también es `NULL` cuando se trata de un tipo de geometría no admitido.

Se valida la estructura del documento: el `type` de nivel superior debe ser `FeatureCollection` y cada elemento de `features` debe tener `type` `Feature`. De forma predeterminada, las coordenadas deben cumplir las reglas estructurales de GeoJSON: un `LineString` (y cada línea de un `MultiLineString`) debe tener al menos dos puntos, y un anillo de `Polygon` (y cada anillo de un `MultiPolygon`) debe estar cerrado y tener al menos cuatro puntos (consulta [validación de geometrías](#geometry-validation)). Los documentos malformados se rechazan en lugar de cargarse silenciosamente.

El orden de las claves es flexible: el `type` de nivel superior puede aparecer antes o después del array `features`, y dentro de un objeto de geometría `coordinates` puede aparecer antes o después de `type`.

La inferencia de esquema devuelve el esquema fijo anterior, por lo que `DESCRIBE` y `SELECT ... FROM format(...)` funcionan sin una definición de tabla.

Dado el siguiente archivo GeoJSON `london.geojson`, que contiene una mezcla de tipos de geometría:

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

Podemos consultar el archivo y examinar los tipos de geometría:

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

La extensión del archivo `.geojson` se detecta automáticamente, por lo que se puede omitir el argumento de formato:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

Podemos usar `variantType` para identificar el tipo subyacente de cada objeto de tipo Geometry:

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

Y podemos extraer los datos subyacentes de esta manera:

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

Al acceder a una subcolumna `Geometry`, se devuelve el valor cuando la fila contiene ese tipo y, en caso contrario, el valor predeterminado del tipo — `(0,0)` para `Point` y `[]` para los tipos basados en arrays —, así que usa `variantType(geometry)` para identificar cuál está establecido.

También podemos ingestar datos GeoJSON en una tabla:

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

A continuación, realiza una consulta por tipo de entidad:

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

También podemos inferir el esquema de los datos en GeoJSON sin una definición de tabla:

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
  ### manejo de tipos de geometría no admitidos
</div>

Algunos tipos de geometría GeoJSON válidos — como `GeometryCollection` y `MultiPoint` — no se pueden representar con el tipo `Geometry` de ClickHouse. Puede controlar qué ocurre cuando una geometría de este tipo debe almacenarse en la columna `geometry` mediante la configuración `input_format_geojson_unsupported_geometry_handling`. Los posibles valores son:

* `'throw'` — lanzar una excepción (predeterminado)
* `'null'` — insertar un valor `NULL` en la columna `geometry` y seguir con el análisis

Este comportamiento se aplica solo cuando se lee la columna `geometry`. Cuando `geometry` no es una columna de salida solicitada (por ejemplo, `SELECT id FROM ...`), una geometría no admitida sigue validándose para comprobar que esté bien formada, pero no activa este comportamiento: ni lanza una excepción ni inserta `NULL`, porque no se materializa ningún valor de geometría.

<div id="reading-limitations">
  ### Limitaciones
</div>

La lectura solo refleja lo que cabe en el esquema fijo, por lo que parte de la información de GeoJSON no se conserva:

* Solo se generan `id`, `geometry` y `properties`; el resto de la estructura del documento no se expone como columnas.
* La tercera coordenada de una posición (elevación), así como cualquier coordenada posterior, se descartan: las posiciones pasan a ser `[longitude, latitude]`.
* `bbox` y los miembros externos (como un `name` o `crs` de nivel superior, o miembros adicionales dentro de una `Feature`) se ignoran.
* Un `id` numérico se almacena como texto, por lo que se pierde la distinción entre cadena y número; un `id` ausente o `null` pasa a ser `NULL`.
* `GeometryCollection` y `MultiPoint` no pueden representarse; consulte [Manejo de los tipos de geometría no admitidos](#unsupported-geometry).

<div id="writing-data">
  ## Escritura de datos
</div>

Escribir un conjunto de resultados genera una única [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) de GeoJSON, con un `Feature` por fila.

Las columnas del resultado se asignan a cada `Feature` de la siguiente manera:

| Miembro de Feature | Se obtiene de                      | Notas                                                                                                                                                                                                                                                                                                                                                         |
| ------------------ | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`             | —                                  | Siempre `"Feature"`.                                                                                                                                                                                                                                                                                                                                          |
| `geometry`         | la única columna de tipo geometría | Se requiere exactamente una columna de tipo geometría; de lo contrario, la consulta se rechaza. Una geometría `NULL` se escribe como `null`.                                                                                                                                                                                                                  |
| `id`               | una columna llamada `id`           | Se omite cuando el valor es `NULL`. Una columna `String` se escribe como una cadena JSON y una columna numérica como un número JSON.                                                                                                                                                                                                                          |
| `properties`       | todas las columnas restantes       | Una única columna llamada `properties` cuyo tipo es similar a un objeto (`JSON`, `Map` o un `Tuple` con nombre) se escribe directamente como el objeto `properties`, en lugar de anidarse bajo una clave `properties`. De lo contrario, cada columna restante se convierte en una propiedad con su nombre como clave (un objeto vacío cuando no hay ninguna). |

La columna de tipo geometría puede ser la variante `Geometry` o un tipo geo específico; cada una se asigna a un tipo de geometría de GeoJSON:

| Tipo de ClickHouse | GeoJSON `"type"`                         |
| ------------------ | ---------------------------------------- |
| `Point`            | `Point`                                  |
| `LineString`       | `LineString`                             |
| `MultiLineString`  | `MultiLineString`                        |
| `Polygon`          | `Polygon`                                |
| `MultiPolygon`     | `MultiPolygon`                           |
| `Ring`             | `Polygon` (un solo anillo)               |
| `Geometry`         | el tipo de la variante activa (o `null`) |

`Ring` no es un tipo de geometría de GeoJSON: un [anillo lineal](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) es un componente de un `Polygon`, por lo que un valor `Ring` se escribe como un `Polygon` de un solo anillo.

<div id="writing-examples">
  ### Ejemplos
</div>

Siguiendo con la tabla `london` [creada anteriormente](#reading-data), exportar columnas de atributos simples convierte en una propiedad cada columna distinta de `id` y `geometry`:

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

Como una única columna de tipo objeto llamada `properties` se escribe directamente, al leer un archivo GeoJSON y volver a escribirlo tal cual se reproduce el documento (las columnas `id`, `geometry` y `properties` son las que se infieren del archivo):

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

Una columna `id` numérica se representa como un número JSON (un `id` `Nullable` que es `NULL` se omite por completo):

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

Un `Ring` se representa como un `Polygon` de un solo anillo:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### Escritura en un archivo
</div>

Use `INTO OUTFILE` para escribir un archivo GeoJSON desde el client:

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

El servidor puede escribir el archivo directamente con la función de tabla `file` (la extensión `.geojson` selecciona automáticamente el formato):

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="reading-limitations">
  ### Limitaciones
</div>

:::note
Los tipos geo de ClickHouse no incluyen ningún sistema de referencia de coordenadas, por lo que la salida asume que las coordenadas ya están en WGS84 como longitud/latitud en el orden `[longitude, latitude]`, tal como exige la [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4). No se realiza ninguna reproyección ni intercambio de ejes, por lo que las coordenadas proyectadas —o los datos almacenados como `(latitude, longitude)`— producen un GeoJSON estructuralmente válido, pero no conforme.
:::

La salida refleja únicamente lo que almacena ClickHouse:

* La información descartada durante la lectura —la elevación de una posición, `bbox`, miembros adicionales y la distinción entre cadena y número en un `id`— no puede reproducirse; consulta [Limitaciones de lectura](#reading-limitations).
* Las coordenadas se escriben a partir de valores `Float64` usando su representación más corta que permite recuperar exactamente el valor al volver a leerlo.
* Un objeto `properties` tomado directamente de una columna `JSON` se emite en el orden canónico de claves del tipo `JSON`, que puede diferir del de la entrada.

Las geometrías se escriben exactamente tal como se almacenan: se conservan el orden de las coordenadas y la orientación. De forma predeterminada, se valida la forma GeoJSON durante la escritura (consulta [validación de geometrías](#geometry-validation)): una geometría que no sea una forma GeoJSON válida, como un `LineString` con un solo punto o un anillo de `Polygon` sin cerrar, se rechaza para que el documento escrito pueda volver a leerse correctamente. Establece `format_geojson_validate_geometry = 0` para emitir esas geometrías tal cual, lo que produce un GeoJSON estructuralmente válido, pero no conforme. La invariante de la regla de la mano derecha (orientación) no se aplica en ningún caso, y se conserva la distinción entre `null` y un objeto `properties` vacío.

<div id="geometry-validation">
  ## Validación de geometrías
</div>

La configuración `format_geojson_validate_geometry` controla si el formato aplica las reglas de forma geométrica de [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1) en ambos sentidos. Está habilitada de forma predeterminada.

Cuando está habilitada, se rechaza cualquier geometría que infrinja las reglas de forma de GeoJSON: un `LineString` (o una línea de un `MultiLineString`) con menos de dos puntos; un anillo de `Polygon` o `MultiPolygon` con menos de cuatro puntos, o cuyo primer y último punto sean distintos (un anillo no cerrado); o un `MultiLineString`, `Polygon` o `MultiPolygon` vacío. Las mismas reglas se aplican tanto al leer un documento de este tipo como al escribir un valor de ClickHouse de este tipo, por lo que un documento escrito siempre puede volver a leerse.

Cuando está deshabilitada, estas reglas de forma no se aplican en ninguno de los dos sentidos: las geometrías degeneradas se leen y se escriben tal cual. Esto permite que valores geométricos de ClickHouse que no son geometrías GeoJSON válidas se conserven intactos al pasar por el formato, a costa de producir documentos que no son GeoJSON válidos.

La validación es solo estructural: comprueba el recuento de puntos y el cierre de los anillos. No examina la corrección geométrica de una forma, por lo que se acepta una geometría estructuralmente válida pero geométricamente degenerada en cualquiera de los dos sentidos; por ejemplo, un polígono de área cero, un anillo que se autointerseca o un polígono cuyos huecos (anillos interiores) están fuera de su anillo exterior. Del mismo modo, tampoco se aplica nunca la orientación de los anillos de los polígonos según la regla de la mano derecha (winding).

Hay una comprobación que es independiente de esta configuración: las coordenadas no finitas (`NaN`, `Inf`) siempre se rechazan, porque no pueden representarse como números JSON.