---
description: 'Configuración de LIFETIME del diccionario para la actualización automática'
sidebar_label: 'LIFETIME'
sidebar_position: 5
slug: /sql-reference/statements/create/dictionary/lifetime
title: 'Actualización de los datos del diccionario mediante LIFETIME'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

ClickHouse actualiza periódicamente los diccionarios según la etiqueta `LIFETIME` (definida en segundos).
`LIFETIME` es el intervalo de actualización de los diccionarios descargados por completo y el intervalo de invalidación de los diccionarios en caché.

Durante las actualizaciones, la versión anterior de un diccionario puede seguir consultándose.
Las actualizaciones de los diccionarios no bloquean las consultas, salvo cuando se cargan por primera vez.
Si se produce un error durante una actualización, este se escribe en el registro del servidor, y las consultas pueden seguir usando la versión anterior del diccionario.
Si la actualización de un diccionario se realiza correctamente, la versión anterior del diccionario se reemplaza [de forma atómica](/es/concepts/glossary#atomicity).

Ejemplo de configuración:

<CloudDetails />

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

o

```sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Establecer `<lifetime>0</lifetime>` (`LIFETIME(0)`) impide que los diccionarios se actualicen.

Puede establecer un intervalo de tiempo para las actualizaciones, y ClickHouse elegirá un instante aleatorio uniforme dentro de este rango. Esto es necesario para distribuir la carga sobre el origen del diccionario al actualizarse en un gran número de servidores.

Ejemplo de configuración:

```xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

o

```sql
LIFETIME(MIN 300 MAX 360)
```

Si `<min>0</min>` y `<max>0</max>`, ClickHouse no recarga el diccionario según el tiempo de espera.
En este caso, ClickHouse puede recargar el diccionario antes si se modificó el archivo de configuración del diccionario o si se ejecutó el comando `SYSTEM RELOAD DICTIONARY`.

Al actualizar los diccionarios, el servidor de ClickHouse aplica una lógica distinta según el tipo de [fuente](./sources/):

* En el caso de un archivo de texto, comprueba la hora de modificación. Si difiere de la registrada anteriormente, el diccionario se actualiza.
* De forma predeterminada, los diccionarios de otras fuentes se actualizan siempre.

Para otras fuentes (ODBC, PostgreSQL, ClickHouse, etc.), puede configurar una consulta para que los diccionarios se actualicen solo si realmente han cambiado, en lugar de hacerlo cada vez. Para ello, siga estos pasos:

* La tabla del diccionario debe tener un campo que siempre cambie cuando se actualicen los datos de la fuente.
* La configuración de la fuente debe especificar una consulta que recupere el campo variable. El servidor de ClickHouse interpreta el resultado de la consulta como una fila y, si esta fila ha cambiado con respecto a su estado anterior, el diccionario se actualiza. Especifique la consulta en el campo `<invalidate_query>` de la configuración de la [fuente](./sources/).

Ejemplo de configuración:

```xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

o

```sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

Para los diccionarios `Cache`, `ComplexKeyCache`, `SSDCache` y `SSDComplexKeyCache` se admiten actualizaciones síncronas y asíncronas.

También es posible que los diccionarios `Flat`, `Hashed`, `HashedArray` y `ComplexKeyHashed` soliciten únicamente los datos que hayan cambiado desde la actualización anterior. Si se especifica `update_field` como parte de la configuración del origen del diccionario, se añadirá a la solicitud de datos el valor, en segundos, del momento de la actualización anterior. Según el tipo de origen (Executable, HTTP, MySQL, PostgreSQL, ClickHouse u ODBC), se aplicará una lógica diferente a `update_field` antes de solicitar datos a una fuente externa.

* Si la fuente es HTTP, `update_field` se añadirá como un parámetro de consulta, con la hora de la última actualización como valor del parámetro.
* Si la fuente es Executable, `update_field` se añadirá como un argumento del script ejecutable, con la hora de la última actualización como valor del argumento.
* Si la fuente es ClickHouse, MySQL, PostgreSQL u ODBC, habrá una parte adicional de `WHERE`, en la que `update_field` se compara como mayor o igual que la hora de la última actualización.
  * De forma predeterminada, esta condición `WHERE` se comprueba en el nivel más alto de la consulta SQL. Como alternativa, la condición puede comprobarse en cualquier otra cláusula `WHERE` dentro de la consulta usando la palabra clave `{condition}`. Ejemplo:
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

Si se establece la opción `update_field`, también se puede establecer la opción adicional `update_lag`. El valor de la opción `update_lag` se resta de la hora de la actualización anterior antes de solicitar los datos actualizados.

Ejemplo de configuración:

```xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

o

```sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```