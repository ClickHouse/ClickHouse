# File {#table_engines-file}

El motor de tabla de archivos mantiene los datos en un archivo en uno de los [file
Formato](../../interfaces/formats.md#formats) (TabSeparated, Native, etc.).

Ejemplos de uso:

-   Exportación de datos de ClickHouse a archivo.
-   Convertir datos de un formato a otro.
-   Actualización de datos en ClickHouse mediante la edición de un archivo en un disco.

## Uso en el servidor ClickHouse {#usage-in-clickhouse-server}

``` sql
File(Format)
```

El `Format` parámetro especifica uno de los formatos de archivo disponibles. Realizar
`SELECT` consultas, el formato debe ser compatible para la entrada, y para realizar
`INSERT` consultas – para la salida. Los formatos disponibles se enumeran en el
[Formato](../../interfaces/formats.md#formats) apartado.

ClickHouse no permite especificar la ruta del sistema de archivos para`File`. Utilizará la carpeta definida por [camino](../server_settings/settings.md) configuración en la configuración del servidor.

Al crear una tabla usando `File(Format)` crea un subdirectorio vacío en esa carpeta. Cuando los datos se escriben en esa tabla, se colocan en `data.Format` en ese subdirectorio.

Puede crear manualmente esta subcarpeta y archivo en el sistema de archivos del servidor y luego [CONECTAR](../../query_language/misc.md) para mostrar información con el nombre coincidente, para que pueda consultar datos desde ese archivo.

!!! warning "Advertencia"
    Tenga cuidado con esta funcionalidad, ya que ClickHouse no realiza un seguimiento de los cambios externos en dichos archivos. El resultado de las escrituras simultáneas a través de ClickHouse y fuera de ClickHouse no está definido.

**Ejemplo:**

**1.** Configurar el `file_engine_table` tabla:

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

Por defecto, ClickHouse creará una carpeta `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Crear manualmente `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` contener:

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Consultar los datos:

``` sql
SELECT * FROM file_engine_table
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Uso es Clickhouse-local {#usage-in-clickhouse-local}

En [Sistema abierto.](../utils/clickhouse-local.md) El motor de archivos acepta la ruta del archivo además de `Format`. Los flujos de entrada / salida predeterminados se pueden especificar utilizando nombres numéricos o legibles por humanos como `0` o `stdin`, `1` o `stdout`.
**Ejemplo:**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## Detalles de la implementación {#details-of-implementation}

-   Multiple `SELECT` las consultas se pueden realizar simultáneamente, pero `INSERT` las consultas se esperarán entre sí.
-   Apoyado la creación de nuevos archivos por `INSERT` consulta.
-   Si el archivo existe, `INSERT` añadiría nuevos valores en él.
-   No soportado:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   Indice
    -   Replicación

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/file/) <!--hide-->
