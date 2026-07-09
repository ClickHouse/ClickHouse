---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Diccionarios de polígonos'
sidebar_label: 'Polígono'
sidebar_position: 12
description: 'Configura diccionarios de polígonos para búsquedas de puntos en polígonos.'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

El diccionario `polygon` (`POLYGON`) está optimizado para consultas de punto en polígono, esencialmente búsquedas de &quot;geocodificación inversa&quot;.
Dada una coordenada (latitud/longitud), encuentra de forma eficiente qué polígono o región (dentro de un conjunto de muchos polígonos, como fronteras de países o regiones) contiene ese punto.
Es especialmente útil para asociar coordenadas geográficas con la región a la que pertenecen.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="Diccionarios de polígonos en ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

Ejemplo de configuración de un diccionario `polygon`:

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

  <TabItem value="xml" label="Archivo de configuración">
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

Al configurar el diccionario `polygon`, la clave debe tener uno de estos dos tipos:

* Un polígono simple. Es un array de puntos.
* MultiPolygon. Es un array de polígonos. Cada polígono es un array bidimensional de puntos. El primer elemento de este array es el contorno exterior del polígono, y los elementos posteriores especifican las áreas que deben excluirse de él.

Los puntos pueden especificarse como un array o una tupla de sus coordenadas. En la implementación actual, solo se admiten puntos bidimensionales.

Puede cargar sus propios datos en cualquiera de los formatos compatibles con ClickHouse.

Hay 3 tipos de [almacenamiento en memoria](./#storing-dictionaries-in-memory) disponibles:

| Layout               | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `POLYGON_SIMPLE`     | Implementación básica. Para cada consulta, se recorre linealmente todos los polígonos y se comprueba la pertenencia sin índices adicionales.                                                                                                                                                                                                                                                                                         |
| `POLYGON_INDEX_EACH` | Se crea un índice independiente para cada polígono, lo que permite comprobaciones rápidas de pertenencia en la mayoría de los casos (optimizado para regiones geográficas). Se superpone una cuadrícula sobre el área, dividiendo recursivamente las celdas en 16 partes iguales. La división se detiene cuando la profundidad de la recursión alcanza `MAX_DEPTH` o una celda intersecta como máximo `MIN_INTERSECTIONS` polígonos. |
| `POLYGON_INDEX_CELL` | También crea la cuadrícula descrita anteriormente con las mismas opciones. Para cada celda hoja, se construye un índice sobre todos los fragmentos de polígono que caen en ella, lo que permite responder rápidamente a las consultas.                                                                                                                                                                                               |
| `POLYGON`            | Sinónimo de `POLYGON_INDEX_CELL`.                                                                                                                                                                                                                                                                                                                                                                                                    |

Las consultas de diccionario se realizan mediante [funciones](/es/sql-reference/functions/ext-dict-functions.md) estándar para trabajar con diccionarios.
Una diferencia importante es que aquí las claves serán los puntos para los que desea encontrar el polígono que los contiene.

**Ejemplo**

Ejemplo de uso del diccionario definido anteriormente:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

Como resultado de ejecutar el último comando, para cada punto de la tabla &#39;points&#39; se encontrará un polígono de área mínima que contenga ese punto y se mostrarán los atributos solicitados.

**Ejemplo**

Puede leer columnas de diccionarios de polígonos mediante una consulta SELECT; solo tiene que activar `store_polygon_key_column = 1` en la configuración del diccionario o en la consulta DDL correspondiente.

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