---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: Actualizaciones del diccionario
---

# Actualizaciones del diccionario {#dictionary-updates}

ClickHouse actualiza periódicamente los diccionarios. El intervalo de actualización para los diccionarios completamente descargados y el intervalo de invalidación para los diccionarios almacenados en caché se `<lifetime>` etiqueta en segundos.

Las actualizaciones del diccionario (aparte de la carga para el primer uso) no bloquean las consultas. Durante las actualizaciones, se utiliza la versión anterior de un diccionario. Si se produce un error durante una actualización, el error se escribe en el registro del servidor y las consultas continúan utilizando la versión anterior de los diccionarios.

Ejemplo de configuración:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Configuración `<lifetime>0</lifetime>` (`LIFETIME(0)`) impide que los diccionarios se actualicen.

Puede establecer un intervalo de tiempo para las actualizaciones, y ClickHouse elegirá un tiempo uniformemente aleatorio dentro de este rango. Esto es necesario para distribuir la carga en la fuente del diccionario cuando se actualiza en una gran cantidad de servidores.

Ejemplo de configuración:

``` xml
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

``` sql
LIFETIME(MIN 300 MAX 360)
```

Si `<min>0</min>` y `<max>0</max>`, ClickHouse no vuelve a cargar el diccionario por tiempo de espera.
En este caso, ClickHouse puede volver a cargar el diccionario anteriormente si el archivo de configuración del diccionario `SYSTEM RELOAD DICTIONARY` se ejecutó el comando.

Al actualizar los diccionarios, el servidor ClickHouse aplica una lógica diferente según el tipo de [fuente](external-dicts-dict-sources.md):

Al actualizar los diccionarios, el servidor ClickHouse aplica una lógica diferente según el tipo de [fuente](external-dicts-dict-sources.md):

-   Para un archivo de texto, comprueba el tiempo de modificación. Si la hora difiere de la hora previamente grabada, el diccionario se actualiza.
-   Para las tablas MyISAM, el tiempo de modificación se comprueba utilizando un `SHOW TABLE STATUS` consulta.
-   Los diccionarios de otras fuentes se actualizan cada vez de forma predeterminada.

Para fuentes MySQL (InnoDB), ODBC y ClickHouse, puede configurar una consulta que actualizará los diccionarios solo si realmente cambiaron, en lugar de cada vez. Para ello, siga estos pasos:

-   La tabla del diccionario debe tener un campo que siempre cambie cuando se actualizan los datos de origen.
-   La configuración del origen debe especificar una consulta que recupere el campo de cambio. El servidor ClickHouse interpreta el resultado de la consulta como una fila, y si esta fila ha cambiado en relación con su estado anterior, el diccionario se actualiza. Especifique la consulta en el `<invalidate_query>` en la configuración de la [fuente](external-dicts-dict-sources.md).

Ejemplo de configuración:

``` xml
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

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_lifetime/) <!--hide-->
