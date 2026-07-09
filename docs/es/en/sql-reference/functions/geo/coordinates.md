---
description: 'Documentación sobre coordenadas'
sidebar_label: 'Coordenadas geográficas'
slug: /sql-reference/functions/geo/coordinates
title: 'Funciones para trabajar con coordenadas geográficas'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

Calcula la distancia entre dos puntos de la superficie de la Tierra mediante [la fórmula del círculo máximo](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parámetros de entrada**

* `lon1Deg` — Longitud del primer punto en grados. Rango: `[-180°, 180°]`.
* `lat1Deg` — Latitud del primer punto en grados. Rango: `[-90°, 90°]`.
* `lon2Deg` — Longitud del segundo punto en grados. Rango: `[-180°, 180°]`.
* `lat2Deg` — Latitud del segundo punto en grados. Rango: `[-90°, 90°]`.

Los valores positivos corresponden a la latitud norte y la longitud este, y los valores negativos corresponden a la latitud sur y la longitud oeste.

**Valor devuelto**

La distancia entre dos puntos de la superficie terrestre, en metros.

Genera una excepción cuando los valores de los parámetros de entrada están fuera del rango.

**Ejemplo**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

<div id="geodistance">
  ## geoDistance
</div>

Similar a `greatCircleDistance`, pero calcula la distancia sobre el elipsoide WGS-84 en lugar de sobre una esfera. Se trata de una aproximación más precisa del geoide terrestre.
El rendimiento es el mismo que el de `greatCircleDistance` (sin impacto en el rendimiento). Se recomienda usar `geoDistance` para calcular distancias sobre la Tierra.

Nota técnica: para puntos suficientemente cercanos, calculamos la distancia mediante una aproximación plana con la métrica del plano tangente en el punto medio entre las coordenadas.

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parámetros de entrada**

* `lon1Deg` — Longitud del primer punto en grados. Rango: `[-180°, 180°]`.
* `lat1Deg` — Latitud del primer punto en grados. Rango: `[-90°, 90°]`.
* `lon2Deg` — Longitud del segundo punto en grados. Rango: `[-180°, 180°]`.
* `lat2Deg` — Latitud del segundo punto en grados. Rango: `[-90°, 90°]`.

Los valores positivos corresponden a la latitud norte y la longitud este, y los valores negativos a la latitud sur y la longitud oeste.

**Valor devuelto**

La distancia entre dos puntos de la superficie terrestre, en metros.

Genera una excepción cuando los valores de los parámetros de entrada están fuera del rango.

**Ejemplo**

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

<div id="greatcircleangle">
  ## greatCircleAngle
</div>

Calcula el ángulo central entre dos puntos de la superficie terrestre mediante [la fórmula del círculo máximo](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Parámetros de entrada**

* `lon1Deg` — Longitud del primer punto en grados.
* `lat1Deg` — Latitud del primer punto en grados.
* `lon2Deg` — Longitud del segundo punto en grados.
* `lat2Deg` — Latitud del segundo punto en grados.

**Valor devuelto**

El ángulo central entre dos puntos, en grados.

**Ejemplo**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

<div id="geotoutm">
  ## geoToUTM
</div>

Convierte coordenadas geográficas WGS84 `(longitude, latitude)` en coordenadas [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system).

UTM es un conjunto de 60 proyecciones transversales de Mercator, cada una de las cuales cubre una zona longitudinal de 6°, y transforma coordenadas geográficas en una cuadrícula plana en metros. La zona se selecciona automáticamente a partir de la longitud, aplicando las excepciones estándar para Noruega y Svalbard, salvo que se proporcione una `zone` explícita. UTM solo está definido para latitudes en el rango `[-80°, 84°]`; los casquetes polares utilizan el sistema UPS independiente.

```sql
geoToUTM(longitude, latitude[, zone])
```

**Argumentos**

* `longitude` — Longitud en grados. Rango: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitud en grados. Rango: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `zone` — Opcional. Fuerza el uso de esta zona UTM en lugar de seleccionarla automáticamente. Rango: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valor devuelto**

Una tupla nombrada `(easting, northing, zone, band)`: `easting` y `northing` en metros ([`Float64`](../../data-types/float.md)), el número de zona UTM `zone` ([`UInt8`](../../data-types/int-uint.md)) y la letra de banda de latitud MGRS `band` ([`FixedString(1)`](../../data-types/fixedstring.md)). Un `band` de `'N'` o superior denota el hemisferio norte.

Genera una excepción cuando la latitud está fuera de `[-80°, 84°]` o la longitud está fuera de `[-180°, 180°]`.

**Ejemplo**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

Convierte las coordenadas [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) nuevamente a coordenadas geográficas WGS84 `(longitude, latitude)`. Esta es la inversa de [`geoToUTM`](#geotoutm).

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**Argumentos**

* `easting` — Coordenada este en metros (incluye el falso este de 500000 m). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `northing` — Coordenada norte en metros (incluye el falso norte de 10000000 m en el hemisferio sur). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `zone` — Número de zona UTM. Rango: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).
* `is_north` — Hemisferio: `1` para el hemisferio norte y `0` para el sur. [`(U)Int*`](../../data-types/int-uint.md).

**Valor devuelto**

Una tupla nombrada `(longitude, latitude)` en grados. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Ejemplo**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

Codifica las coordenadas geográficas WGS84 `(longitude, latitude)` en una cadena [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System).

La cadena es `<zone><band><100km square><easting><northing>`, por ejemplo `31UDQ4825111935`. El argumento `precision` controla el número de dígitos usados para `easting` y `northing`: `5` (por defecto) para 1 m, `4` para 10 m, `3` para 100 m, `2` para 1 km, `1` para 10 km y `0` únicamente para la cuadrícula de 100 km. MGRS solo está definido para latitudes en el rango `[-80°, 84°]`.

```sql
geoToMGRS(longitude, latitude[, precision])
```

**Argumentos**

* `longitude` — Longitud en grados. Rango: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitud en grados. Rango: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `precision` — Opcional. Número de dígitos para cada una de las coordenadas este y norte. Valor predeterminado: `5`. Rango: `[0, 5]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valor devuelto**

La cadena de referencia MGRS. [`String`](../../data-types/string.md).

**Ejemplo**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

<div id="mgrstogeo">
  ## MGRSToGeo
</div>

Decodifica una cadena [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) en coordenadas geográficas WGS84 `(longitude, latitude)`. Es la inversa de [`geoToMGRS`](#geotomgrs).

El punto devuelto es el centro del cuadrado de la cuadrícula de referencia, por lo que la precisión del resultado coincide con la precisión codificada en la cadena. Se ignoran los espacios en blanco en la entrada y no se distingue entre mayúsculas y minúsculas en las letras.

```sql
MGRSToGeo(mgrs)
```

**Argumentos**

* `mgrs` — Cadena de referencia MGRS que se debe decodificar. [`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md).

**Valor devuelto**

Una tupla nombrada `(longitude, latitude)` en grados. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Ejemplo**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

Comprueba si el punto pertenece al menos a una de las elipses.
Las coordenadas son geométricas en el sistema de coordenadas cartesianas.

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Parámetros de entrada**

* `x, y` — Coordenadas de un punto en el plano.
* `xᵢ, yᵢ` — Coordenadas del centro de la `i`-ésima elipse.
* `aᵢ, bᵢ` — Ejes de la `i`-ésima elipse en las unidades de las coordenadas `x, y`.

Los parámetros de entrada deben ser `2+4⋅n`, donde `n` es el número de elipses.

**Valores devueltos**

`1` si el punto está dentro de al menos una de las elipses; `0` si no lo está.

**Ejemplo**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

<div id="pointinpolygon">
  ## pointInPolygon
</div>

Comprueba si el punto pertenece al polígono en el plano.

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Valores de entrada**

* `(x, y)` — Coordenadas de un punto en el plano. Tipo de dato — [Tuple](../../data-types/tuple.md) — Una tupla de dos números.
* `[(a, b), (c, d) ...]` — Vértices del polígono. Tipo de dato — [Array](../../data-types/array.md). Cada vértice se representa mediante un par de coordenadas `(a, b)`. Los vértices deben especificarse en sentido horario o antihorario. El número mínimo de vértices es 3. El polígono debe ser constante.
* La función admite polígonos con huecos (secciones recortadas). Tipo de dato — [Polygon](../../data-types/geo.md/#polygon). Pase el `Polygon` completo como segundo argumento, o pase primero el anillo exterior y luego cada hueco como argumento adicional independiente.
* La función también admite multipolígonos. Tipo de dato — [MultiPolygon](../../data-types/geo.md/#multipolygon). Pase el `MultiPolygon` completo como segundo argumento, o proporcione cada polígono componente como un argumento independiente.

**Valores devueltos**

`1` si el punto está dentro del polígono, `0` si no lo está.
Si el punto está en el borde del polígono, la función puede devolver 0 o 1.

**Ejemplo**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **Nota**
> • Puede establecer `validate_polygons = 0` para omitir la validación de geometrías.
> • `pointInPolygon` asume que cada polígono está bien formado. Si la entrada se autointerseca, tiene anillos mal ordenados o aristas que se solapan, los resultados dejan de ser fiables, especialmente para puntos situados exactamente sobre una arista, un vértice o dentro de una autointersección, donde la distinción entre &quot;dentro&quot; y &quot;fuera&quot; no está definida.
> • Cuando el argumento del polígono es constante y el punto se expresa mediante columnas clave indexadas (por ejemplo, `pointInPolygon((x, y), constant_polygon)` en una tabla donde `x, y` forman parte de la `PRIMARY KEY` o están cubiertas por un índice `minmax`), ClickHouse puede usar tanto la clave primaria como los índices de omisión de datos `minmax` para descartar los gránulos irrelevantes.