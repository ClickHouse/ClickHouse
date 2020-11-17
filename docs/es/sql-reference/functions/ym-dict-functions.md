---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: Trabajando con Yandex.Diccionarios de Metrica
---

# Funciones para trabajar con Yandex.Diccionarios de Metrica {#functions-for-working-with-yandex-metrica-dictionaries}

Para que las funciones a continuación funcionen, la configuración del servidor debe especificar las rutas y direcciones para obtener todo el Yandex.Diccionarios Metrica. Los diccionarios se cargan en la primera llamada de cualquiera de estas funciones. Si las listas de referencia no se pueden cargar, se lanza una excepción.

Para obtener información sobre cómo crear listas de referencia, consulte la sección “Dictionaries”.

## Múltiples Geobases {#multiple-geobases}

ClickHouse admite trabajar con múltiples geobases alternativas (jerarquías regionales) simultáneamente, con el fin de soportar diversas perspectivas sobre a qué países pertenecen ciertas regiones.

El ‘clickhouse-server’ config especifica el archivo con la jerarquía regional::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Además de este archivo, también busca archivos cercanos que tengan el símbolo \_ y cualquier sufijo anexado al nombre (antes de la extensión del archivo).
Por ejemplo, también encontrará el archivo `/opt/geo/regions_hierarchy_ua.txt` si está presente.

`ua` se llama la clave del diccionario. Para un diccionario sin un sufijo, la clave es una cadena vacía.

Todos los diccionarios se vuelven a cargar en tiempo de ejecución (una vez cada cierto número de segundos, como se define en el parámetro de configuración builtin\_dictionaries\_reload\_interval , o una vez por hora por defecto). Sin embargo, la lista de diccionarios disponibles se define una vez, cuando se inicia el servidor.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
Ejemplo:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### ¿Cómo puedo hacerlo?\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### ¿Cómo puedo hacerlo?\]) {#regiontoareaid-geobase}

Convierte una región en un área (tipo 5 en la geobase). En todos los demás sentidos, esta función es la misma que ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
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

### ¿Cómo puedo hacerlo?\]) {#regiontodistrictid-geobase}

Convierte una región en un distrito federal (tipo 4 en la geobase). En todos los demás sentidos, esta función es la misma que ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
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

### ¿Cómo puedo hacerlo?\]) {#regiontocountryid-geobase}

Convierte una región en un país. En todos los demás sentidos, esta función es la misma que ‘regionToCity’.
Ejemplo: `regionToCountry(toUInt32(213)) = 225` convierte Moscú (213) a Rusia (225).

### Aquí está el código de identificación.\]) {#regiontocontinentid-geobase}

Convierte una región en un continente. En todos los demás sentidos, esta función es la misma que ‘regionToCity’.
Ejemplo: `regionToContinent(toUInt32(213)) = 10001` convierte Moscú (213) a Eurasia (10001).

### Nuestra misiÃ³n es brindarle un servicio de calidad y confianza a nuestros clientes.) {#regiontotopcontinent-regiontotopcontinent}

Encuentra el continente más alto en la jerarquía de la región.

**Sintaxis**

``` sql
regionToTopContinent(id[, geobase]);
```

**Parámetros**

-   `id` — Region ID from the Yandex geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [Múltiples Geobases](#multiple-geobases). [Cadena](../../sql-reference/data-types/string.md). Opcional.

**Valor devuelto**

-   Identificador del continente de nivel superior (este último cuando subes la jerarquía de regiones).
-   0, si no hay ninguno.

Tipo: `UInt32`.

### Aquí está el código de identificación de la población.\]) {#regiontopopulationid-geobase}

Obtiene la población de una región.
La población se puede registrar en archivos con la geobase. Vea la sección “External dictionaries”.
Si la población no se registra para la región, devuelve 0.
En la geobase de Yandex, la población podría registrarse para las regiones secundarias, pero no para las regiones parentales.

### ¿Cómo puedo hacerlo?\]) {#regioninlhs-rhs-geobase}

Comprueba si un ‘lhs’ región pertenece a un ‘rhs’ regi. Devuelve un número UInt8 igual a 1 si pertenece, o 0 si no pertenece.
The relationship is reflexive – any region also belongs to itself.

### RegiónJerarquía (id\[, geobase\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
Ejemplo: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### ¿Cómo puedo hacerlo?\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn't exist, an empty string is returned.

`ua` y `uk` ambos significan ucraniano.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
