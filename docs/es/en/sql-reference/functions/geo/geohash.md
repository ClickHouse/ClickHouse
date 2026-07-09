---
description: 'Documentación sobre Geohash'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'Funciones para trabajar con Geohash'
doc_type: 'reference'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) es un sistema de geocodificación que subdivide la superficie terrestre en celdas con forma de cuadrícula y codifica cada celda como una cadena corta de letras y dígitos. Es una estructura de datos jerárquica, por lo que, cuanto más larga sea la cadena de geohash, más precisa será la ubicación geográfica.

Si necesitas convertir manualmente coordenadas geográficas a cadenas de geohash, puedes usar [geohash.org](http://geohash.co/)

<div id="geohashencode">
  ## geohashEncode
</div>

Codifica la latitud y la longitud como una cadena de [geohash](#geohash).

**Sintaxis**

```sql
geohashEncode(longitude, latitude, [precision])
```

**Valores de entrada**

* `longitude` — Componente de **longitud** de la coordenada que desea codificar. Número de coma flotante en el intervalo `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude` — Componente de **latitud** de la coordenada que desea codificar. Número de coma flotante en el intervalo `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` (opcional) — Longitud de la cadena codificada resultante. El valor predeterminado es `12`. Entero en el intervalo `[1, 12]`. [Int8](../../data-types/int-uint.md).

:::note

* Todos los parámetros de coordenadas deben ser del mismo tipo: `Float32` o `Float64`.
* En el parámetro `precision`, cualquier valor inferior a `1` o superior a `12` se convierte silenciosamente en `12`.
  :::

**Valores devueltos**

* Cadena alfanumérica de la coordenada codificada (se usa una versión modificada del alfabeto de codificación base32). [String](../../data-types/string.md).

**Ejemplo**

```sql title="Query"
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```text title="Response"
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

<div id="geohashdecode">
  ## geohashDecode
</div>

Decodifica cualquier cadena codificada en [geohash](#geohash) en coordenadas de longitud y latitud.

**Sintaxis**

```sql
geohashDecode(hash_str)
```

**Valores de entrada**

* `hash_str` — Cadena codificada en geohash.

**Valores devueltos**

* Tuple `(longitude, latitude)` con valores `Float64` de longitud y latitud. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**Ejemplo**

```sql
SELECT geohashDecode('ezs42') AS res;
```

```text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

<div id="geohashesinbox">
  ## geohashesInBox
</div>

Devuelve un array de cadenas codificadas con [geohash](#geohash), con la precisión indicada, que quedan dentro de la caja dada e intersectan sus límites; básicamente, una cuadrícula 2D aplanada en un array.

**Sintaxis**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**Argumentos**

* `longitude_min` — Longitud mínima. Rango: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_min` — Latitud mínima. Rango: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `longitude_max` — Longitud máxima. Rango: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_max` — Latitud máxima. Rango: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` — Precisión de geohash. Rango: `[1, 12]`. [UInt8](../../data-types/int-uint.md).

:::note
Todos los parámetros de coordenadas deben ser del mismo tipo: `Float32` o `Float64`.
:::

**Valores devueltos**

* Array de cadenas de la longitud indicada por la precisión, correspondientes a los cuadros de geohash que cubren el área proporcionada; no debe asumirse ningún orden de los elementos. [Array](../../data-types/array.md)([String](../../data-types/string.md)).
* `[]` - Array vacío si los valores mínimos de latitud y longitud no son menores que los valores máximos correspondientes.

:::note
La función genera una excepción si el array resultante supera los 10&#39;000&#39;000 elementos.
:::

**Ejemplo**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```