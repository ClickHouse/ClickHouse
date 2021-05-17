---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Trabajar con coordenadas geogr\xE1ficas"
---

# Funciones para trabajar con coordenadas geográficas {#functions-for-working-with-geographical-coordinates}

## GreatCircleDistance {#greatcircledistance}

Calcule la distancia entre dos puntos en la superficie de la Tierra usando [la fórmula del gran círculo](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parámetros de entrada**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

Los valores positivos corresponden a latitud norte y longitud este, y los valores negativos corresponden a latitud sur y longitud oeste.

**Valor devuelto**

La distancia entre dos puntos en la superficie de la Tierra, en metros.

Genera una excepción cuando los valores de los parámetros de entrada están fuera del intervalo.

**Ejemplo**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses {#pointinellipses}

Comprueba si el punto pertenece al menos a una de las elipses.
Las coordenadas son geométricas en el sistema de coordenadas cartesianas.

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Parámetros de entrada**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`-ésimo puntos suspensivos.
-   `aᵢ, bᵢ` — Axes of the `i`-ésimo puntos suspensivos en unidades de coordenadas x, y.

Los parámetros de entrada deben ser `2+4⋅n`, donde `n` es el número de puntos suspensivos.

**Valores devueltos**

`1` si el punto está dentro de al menos una de las elipses; `0`si no lo es.

**Ejemplo**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointInPolygon {#pointinpolygon}

Comprueba si el punto pertenece al polígono en el plano.

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Valores de entrada**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [Tupla](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Matriz](../../sql-reference/data-types/array.md). Cada vértice está representado por un par de coordenadas `(a, b)`. Los vértices deben especificarse en sentido horario o antihorario. El número mínimo de vértices es 3. El polígono debe ser constante.
-   La función también admite polígonos con agujeros (secciones recortadas). En este caso, agregue polígonos que definan las secciones recortadas utilizando argumentos adicionales de la función. La función no admite polígonos no simplemente conectados.

**Valores devueltos**

`1` si el punto está dentro del polígono, `0` si no lo es.
Si el punto está en el límite del polígono, la función puede devolver 0 o 1.

**Ejemplo**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode {#geohashencode}

Codifica la latitud y la longitud como una cadena geohash, consulte (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Valores de entrada**

-   longitud - longitud parte de la coordenada que desea codificar. Flotando en el rango`[-180°, 180°]`
-   latitude : parte de latitud de la coordenada que desea codificar. Flotando en el rango `[-90°, 90°]`
-   precision - Opcional, longitud de la cadena codificada resultante, por defecto es `12`. Entero en el rango `[1, 12]`. Cualquier valor menor que `1` o mayor que `12` se convierte silenciosamente a `12`.

**Valores devueltos**

-   alfanumérico `String` de coordenadas codificadas (se utiliza la versión modificada del alfabeto de codificación base32).

**Ejemplo**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

Decodifica cualquier cadena codificada por geohash en longitud y latitud.

**Valores de entrada**

-   encoded string - cadena codificada geohash.

**Valores devueltos**

-   (longitud, latitud) - 2-tupla de `Float64` valores de longitud y latitud.

**Ejemplo**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3 {#geotoh3}

Devoluciones [H3](https://uber.github.io/h3/#/documentation/overview/introduction) índice de punto `(lon, lat)` con la resolución especificada.

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) es un sistema de indexación geográfica donde la superficie de la Tierra se divide en incluso azulejos hexagonales. Este sistema es jerárquico, es decir, cada hexágono en el nivel superior se puede dividir en siete incluso pero más pequeños y así sucesivamente.

Este índice se utiliza principalmente para ubicaciones de bucketing y otras manipulaciones geoespaciales.

**Sintaxis**

``` sql
geoToH3(lon, lat, resolution)
```

**Parámetros**

-   `lon` — Longitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Número de índice hexagonal.
-   0 en caso de error.

Tipo: `UInt64`.

**Ejemplo**

Consulta:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

Resultado:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## Información adicional {#geohashesinbox}

Devuelve una matriz de cadenas codificadas por geohash de precisión dada que caen dentro e intersecan los límites de un cuadro dado, básicamente una cuadrícula 2D aplanada en una matriz.

**Valores de entrada**

-   longitude_min - longitud mínima, valor flotante en el rango `[-180°, 180°]`
-   latitude_min - latitud mínima, valor flotante en el rango `[-90°, 90°]`
-   longitude_max - longitud máxima, valor flotante en el rango `[-180°, 180°]`
-   latitude_max - latitud máxima, valor flotante en el rango `[-90°, 90°]`
-   precisión - precisión del geohash, `UInt8` en el rango `[1, 12]`

Tenga en cuenta que todos los parámetros de coordenadas deben ser del mismo tipo: `Float32` o `Float64`.

**Valores devueltos**

-   matriz de cadenas de precisión largas de geohash-cajas que cubren el área proporcionada, no debe confiar en el orden de los artículos.
-   \[\] - matriz vacía si *minuto* valores de *latitud* y *longitud* no son menos que los correspondientes *máximo* valor.

Tenga en cuenta que la función arrojará una excepción si la matriz resultante tiene más de 10'000'000 de elementos.

**Ejemplo**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## h3GetBaseCell {#h3getbasecell}

Devuelve el número de celda base del índice.

**Sintaxis**

``` sql
h3GetBaseCell(index)
```

**Parámetros**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Número de celda base hexagonal. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell
```

Resultado:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## H3HexAreaM2 {#h3hexaream2}

Área hexagonal promedio en metros cuadrados a la resolución dada.

**Sintaxis**

``` sql
h3HexAreaM2(resolution)
```

**Parámetros**

-   `resolution` — Index resolution. Range: `[0, 15]`. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Area in m². Type: [Float64](../../sql-reference/data-types/float.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3HexAreaM2(13) as area
```

Resultado:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## h3IndexesAreNeighbors {#h3indexesareneighbors}

Devuelve si los H3Indexes proporcionados son vecinos o no.

**Sintaxis**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**Parámetros**

-   `index1` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Devoluciones `1` si los índices son vecinos, `0` de lo contrario. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n
```

Resultado:

``` text
┌─n─┐
│ 1 │
└───┘
```

## H3ToChildren {#h3tochildren}

Devuelve una matriz con los índices secundarios del índice dado.

**Sintaxis**

``` sql
h3ToChildren(index, resolution)
```

**Parámetros**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Matriz con los índices H3 hijo. Matriz de tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children
```

Resultado:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## H3ToParent {#h3toparent}

Devuelve el índice primario (más grueso) que contiene el índice dado.

**Sintaxis**

``` sql
h3ToParent(index, resolution)
```

**Parámetros**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Índice padre H3. Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent
```

Resultado:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## H3ToString {#h3tostring}

Convierte la representación H3Index del índice en la representación de cadena.

``` sql
h3ToString(index)
```

**Parámetros**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Representación de cadena del índice H3. Tipo: [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3ToString(617420388352917503) as h3_string
```

Resultado:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## stringToH3 {#stringtoh3}

Convierte la representación de cadena en representación H3Index (UInt64).

``` sql
stringToH3(index_str)
```

**Parámetros**

-   `index_str` — String representation of the H3 index. Type: [Cadena](../../sql-reference/data-types/string.md).

**Valores devueltos**

-   Número de índice hexagonal. Devuelve 0 en caso de error. Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT stringToH3('89184926cc3ffff') as index
```

Resultado:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## h3GetResolution {#h3getresolution}

Devuelve la resolución del índice.

**Sintaxis**

``` sql
h3GetResolution(index)
```

**Parámetros**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

-   Resolución del índice. Gama: `[0, 15]`. Tipo: [UInt8](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT h3GetResolution(617420388352917503) as res
```

Resultado:

``` text
┌─res─┐
│   9 │
└─────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/geo/) <!--hide-->
