---
description: 'Documentación de funciones para trabajar con diccionarios integrados'
sidebar_label: 'Diccionario integrado'
slug: /sql-reference/functions/ym-dict-functions
title: 'Funciones para trabajar con diccionarios integrados'
doc_type: 'reference'
---

:::note
Para que las funciones siguientes funcionen, la configuración del servidor debe especificar las rutas y direcciones para obtener todos los diccionarios integrados. Los diccionarios se cargan en la primera llamada a cualquiera de estas funciones. Si no se pueden cargar las listas de referencia, se produce una excepción.

Por ello, los ejemplos que se muestran en esta sección producirán una excepción en [ClickHouse Fiddle](https://fiddle.clickhouse.com/) y en las implementaciones de quick release y de producción de forma predeterminada, a menos que se configuren previamente.
:::

Para obtener información sobre cómo crear listas de referencia, consulte la sección [&quot;Diccionarios&quot;](../statements/create/dictionary/embedded).

<div id="multiple-geobases">
  ## Múltiples geobases
</div>

ClickHouse permite trabajar simultáneamente con varias geobases alternativas (jerarquías regionales), para contemplar distintas perspectivas sobre a qué países pertenecen determinadas regiones.

La configuración de `clickhouse-server` especifica el archivo con la jerarquía regional:

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Además de este archivo, también busca archivos cercanos que tengan el símbolo `_` y cualquier sufijo añadido al nombre (antes de la extensión del archivo).
Por ejemplo, también encontrará el archivo `/opt/geo/regions_hierarchy_ua.txt`, si está presente. Aquí, `ua` se denomina clave del diccionario. En un diccionario sin sufijo, la clave es una cadena vacía.

Todos los diccionarios se vuelven a cargar en tiempo de ejecución (una vez cada cierto número de segundos, según se define en el parámetro de configuración [`builtin_dictionaries_reload_interval`](/es/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval), o una vez por hora de forma predeterminada). Sin embargo, la lista de diccionarios disponibles se define una sola vez, cuando se inicia el servidor.

Todas las funciones para trabajar con regiones tienen un argumento opcional al final: la clave del diccionario. A esto se le llama geobase.

Ejemplo:

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

Acepta un ID de región y una geobase, y devuelve una cadena con el nombre de la región en el idioma correspondiente. Si no existe una región con el ID especificado, se devuelve una cadena vacía.

**Sintaxis**

```sql
regionToName(id\[, lang\])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* Nombre de la región en el idioma correspondiente especificado por `geobase`. [String](../data-types/string).
* En caso contrario, una cadena vacía.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

Acepta un ID de región de geobase. Si esta región es una ciudad o forma parte de una ciudad, devuelve el ID de región de la ciudad correspondiente. En caso contrario, devuelve 0.

**Sintaxis**

```sql
regionToCity(id [, geobase])
```

**Parámetros**

* `id` — ID de región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulta [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* ID de región de la ciudad correspondiente, si existe. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```response title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
```

### regionToArea

Convierte una región en un área (tipo 5 de la geobase). En todo lo demás, esta función es igual que [&#39;regionToCity&#39;](#regiontocity).

**Sintaxis**

```sql
regionToArea(id [, geobase])
```

**Parámetros**

* `id` — ID de región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* ID de la región del área correspondiente, si existe. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict

Convierte una región en un distrito federal (tipo 4 en geobase). Por lo demás, esta función es igual que &#39;regionToCity&#39;.

**Sintaxis**

```sql
regionToDistrict(id [, geobase])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* ID de la región de la ciudad correspondiente, si existe. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry

Convierte una región en un país (tipo 3 en geobase). En todo lo demás, esta función es igual que &#39;regionToCity&#39;.

**Sintaxis**

```sql
regionToCountry(id [, geobase])
```

**Parámetros**

* `id` — ID de región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulte [Múltiples Geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* ID de región del país correspondiente, si existe. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
```

### regionToContinent

Convierte una región en un continente (tipo 1 en la geobase). Por lo demás, esta función es igual que &#39;regionToCity&#39;.

**Sintaxis**

```sql
regionToContinent(id [, geobase])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulta [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* ID de la región del continente correspondiente, si existe. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
```

### regionToTopContinent

Devuelve el continente de nivel superior en la jerarquía para la región.

**Sintaxis**

```sql
regionToTopContinent(id[, geobase])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* Identificador del continente del nivel superior (es decir, el último al ascender por la jerarquía de regiones). [UInt32](../data-types/int-uint).
* 0, si no existe ninguno.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
```

### regionToPopulation

Obtiene la población de una región. La población puede estar registrada en archivos con la geobase. Consulte la sección [&quot;Diccionarios&quot;](../statements/create/dictionary/embedded). Si la población no está registrada para la región, devuelve 0. En la geobase, la población puede estar registrada para las regiones hijas, pero no para las regiones padre.

**Sintaxis**

```sql
regionToPopulation(id[, geobase])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* Población de la región. [UInt32](../data-types/int-uint).
* 0, si no existe.

**Ejemplo**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
```

### regionIn

Comprueba si una región `lhs` pertenece a una región `rhs`. Devuelve un número UInt8 igual a 1 si pertenece, o a 0 si no.

**Sintaxis**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**Parámetros**

* `lhs` — ID de la región lhs de la geobase. [UInt32](../data-types/int-uint).
* `rhs` — ID de la región rhs de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* 1, si pertenece. [UInt8](../data-types/int-uint).
* 0, si no pertenece.

**Detalles de implementación**

La relación es reflexiva: toda región también pertenece a sí misma.

**Ejemplo**

```sql title="Query"
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

```text title="Response"
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder    
```

### regionHierarchy

Acepta un número UInt32: el ID de región de la geobase. Devuelve un Array de ID de región que consta de la región proporcionada y de todas sus regiones padre en la cadena.

**Sintaxis**

```sql
regionHierarchy(id\[, geobase\])
```

**Parámetros**

* `id` — ID de la región de la geobase. [UInt32](../data-types/int-uint).
* `geobase` — Clave del diccionario. Consulte [Múltiples geobases](#multiple-geobases). [String](../data-types/string). Opcional.

**Valor devuelto**

* Array de ID de región formado por la región proporcionada y todas sus regiones padre a lo largo de la cadena. [Array](../data-types/array)([UInt32](../data-types/int-uint)).

**Ejemplo**

```sql title="Query"
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

```text title="Response"
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

{/* 
  El contenido interno de las siguientes etiquetas se sustituye durante la compilación del framework de documentación por 
  documentación generada a partir de system.functions. Por favor, no modifique ni elimine las etiquetas.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }