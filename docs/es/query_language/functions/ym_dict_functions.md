# Funciones para trabajar con Yandex.Métrica, diccionarios {#functions-for-working-with-yandex-metrica-dictionaries}

Para que las funciones a continuación funcionen, la configuración del servidor debe especificar las rutas y direcciones para obtener todo el Yandex.Diccionarios Metrica. Los diccionarios se cargan en la primera llamada de cualquiera de estas funciones. Si no se pueden cargar las listas de referencia, se produce una excepción.

Para obtener información sobre cómo crear listas de referencia, consulte la sección “Dictionaries”.

## Múltiples geobases {#multiple-geobases}

ClickHouse admite trabajar con múltiples geobases alternativas (jerarquías regionales) simultáneamente, con el fin de soportar diversas perspectivas sobre a qué países pertenecen ciertas regiones.

El ‘clickhouse-server’ config especifica el archivo con la jerarquía regional::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Además de este archivo, también busca archivos cercanos que tengan el símbolo \_ y cualquier sufijo anexado al nombre (antes de la extensión del archivo).
Por ejemplo, también encontrará el archivo `/opt/geo/regions_hierarchy_ua.txt` si está presente.

`ua` se llama la clave del diccionario. Para un diccionario sin un sufijo, la clave es una cadena vacía.

Todos los diccionarios se vuelven a cargar en tiempo de ejecución (una vez cada cierto número de segundos, como se define en el parámetro de configuración builtin\_dictionaries\_reload\_interval , o una vez por hora por defecto). Sin embargo, la lista de diccionarios disponibles se define una vez, cuando se inicia el servidor.

Todas las funciones para trabajar con regiones tienen un argumento opcional al final: la clave del diccionario. Se conoce como la geobase.
Ejemplo:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### ¿Cómo puedo hacerlo?\]) {#regiontocityid-geobase}

Acepta un número UInt32: el ID de región de la geobase de Yandex. Si esta región es una ciudad o parte de una ciudad, devuelve el ID de región para la ciudad apropiada. De lo contrario, devuelve 0.

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

### Aquí está el código de identificación de la población.\]) {#regiontopopulationid-geobase}

Obtiene la población de una región.
La población se puede registrar en archivos con la geobase. Vea la sección “External dictionaries”.
Si la población no se registra para la región, devuelve 0.
En la geobase de Yandex, la población podría registrarse para las regiones secundarias, pero no para las regiones parentales.

### ¿Cómo puedo hacerlo?\]) {#regioninlhs-rhs-geobase}

Comprueba si un ‘lhs’ región pertenece a un ‘rhs’ regi. Devuelve un número UInt8 igual a 1 si pertenece, o 0 si no pertenece.
La relación es reflexiva: cualquier región también pertenece a sí misma.

### RegiónJerarquía (id\[, geobase\]) {#regionhierarchyid-geobase}

Acepta un número UInt32: el ID de región de la geobase de Yandex. Devuelve una matriz de ID de región que consiste en la región pasada y todos los elementos primarios a lo largo de la cadena.
Ejemplo: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### ¿Cómo puedo hacerlo?\]) {#regiontonameid-lang}

Acepta un número UInt32: el ID de región de la geobase de Yandex. Una cadena con el nombre del idioma se puede pasar como un segundo argumento. Los idiomas soportados son: ru, en, ua, uk, by, kz, tr. Si se omite el segundo argumento, el idioma ‘ru’ se utiliza. Si el idioma no es compatible, se produce una excepción. Devuelve una cadena: el nombre de la región en el idioma correspondiente. Si la región con el ID especificado no existe, se devuelve una cadena vacía.

`ua` y `uk` ambos significan ucraniano.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/ym_dict_functions/) <!--hide-->
