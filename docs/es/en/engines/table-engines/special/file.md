---
description: 'El motor de tabla File almacena los datos en un archivo en uno de los formatos de archivo compatibles (`TabSeparated`, `Native`, etc.).'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'Motor de tabla File'
doc_type: 'reference'
---

El motor de tabla File almacena los datos en un archivo en uno de los [formatos de archivo](/es/interfaces/formats#formats-overview) compatibles (`TabSeparated`, `Native`, etc.).

Escenarios de uso:

* Exportación de datos de ClickHouse a un archivo.
* Conversión de datos de un formato a otro.
* Actualización de datos en ClickHouse mediante la edición de un archivo en un disco.

:::note
Este motor no está disponible actualmente en ClickHouse Cloud; [usa en su lugar la función de tabla S3](/es/sql-reference/table-functions/s3.md).
:::

<div id="usage-in-clickhouse-server">
  ## Uso en ClickHouse Server
</div>

```sql
File(Format)
```

El parámetro `Format` especifica uno de los formatos de archivo disponibles. Para realizar
consultas `SELECT`, el formato debe ser compatible con la entrada y, para realizar
consultas `INSERT`, con la salida. Los formatos disponibles se enumeran en la
sección [Formatos](/es/interfaces/formats#formats-overview).

ClickHouse no permite especificar una ruta del sistema de archivos para `File`. Usará la carpeta definida por la opción [path](../../../operations/server-configuration-parameters/settings.md) de la configuración del servidor.

Al crear una tabla con `File(Format)`, se crea un subdirectorio vacío en esa carpeta. Cuando se escriben datos en esa tabla, se guardan en el archivo `data.Format` de ese subdirectorio.

Puede crear manualmente esta subcarpeta y este archivo en el sistema de archivos del servidor y luego hacer [ATTACH](../../../sql-reference/statements/attach.md) a la información de tabla con el nombre correspondiente, para poder consultar los datos de ese archivo.

:::note
Tenga cuidado con esta funcionalidad, porque ClickHouse no realiza un seguimiento de los cambios externos en esos archivos. El resultado de escrituras simultáneas a través de ClickHouse y fuera de ClickHouse no está definido.
:::

<div id="example">
  ## Ejemplo
</div>

**1.** Configure la tabla `file_engine_table`:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

De forma predeterminada, ClickHouse creará la carpeta `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Cree manualmente `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` con el siguiente contenido:

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Consulta los datos:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## Uso en ClickHouse-local
</div>

En [clickhouse-local](../../../operations/utilities/clickhouse-local.md), el motor File acepta una ruta de archivo además de `Format`. Los flujos de entrada/salida predeterminados pueden especificarse con nombres numéricos o legibles para humanos, como `0` o `stdin`, `1` o `stdout`. Es posible leer y escribir archivos comprimidos en función de un parámetro adicional del motor o de la extensión del archivo (`gz`, `br` o `xz`).

**Ejemplo:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## Detalles de la implementación
</div>

* Se pueden realizar varias consultas `SELECT` de forma concurrente, pero las consultas `INSERT` tendrán que esperar unas a otras.
* Se admite crear un archivo nuevo mediante una consulta `INSERT`.
* Si el archivo existe, `INSERT` añadirá nuevos valores al final.
* No se admiten:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * Índices
  * Replicación

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — Opcional. Es posible crear archivos separados particionando los datos según una clave de partición. En la mayoría de los casos, no necesita una clave de partición y, si la necesita, por lo general no hace falta que sea más granular que por mes. La partición no acelera las consultas (a diferencia de la expresión `ORDER BY`). Nunca debe usar una partición demasiado granular. No particione sus datos por identificadores o nombres de client (en su lugar, haga que el identificador o el nombre del client sea la primera columna de la expresión `ORDER BY`).

Para particionar por mes, use la expresión `toYYYYMM(date_column)`, donde `date_column` es una columna con una fecha del tipo [Date](/es/sql-reference/data-types/date.md). Los nombres de las particiones aquí tienen el formato `"YYYYMM"`.

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_path` — Ruta del archivo. Tipo: `LowCardinality(String)`.
* `_file` — Nombre del archivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamaño del archivo en bytes. Tipo: `Nullable(UInt64)`. Si se desconoce el tamaño, el valor es `NULL`.
* `_time` — Hora de la última modificación del archivo. Tipo: `Nullable(DateTime)`. Si se desconoce la hora, el valor es `NULL`.

<div id="settings">
  ## Configuración
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/es/operations/settings/settings#engine_file_empty_if_not_exists) - permite leer datos vacíos de un archivo que no existe. Deshabilitado de forma predeterminada.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/es/operations/settings/settings#engine_file_truncate_on_insert) - permite truncar el archivo antes de insertar datos en él. Deshabilitado de forma predeterminada.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/es/operations/settings/settings.md#engine_file_allow_create_multiple_files) - permite crear un archivo nuevo en cada inserción si el formato tiene un sufijo. Deshabilitado de forma predeterminada.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/es/operations/settings/settings.md#engine_file_skip_empty_files) - permite omitir archivos vacíos durante la lectura. Deshabilitado de forma predeterminada.
* [storage&#95;file&#95;read&#95;method](/es/operations/settings/settings#engine_file_empty_if_not_exists) - método para leer datos desde el archivo de almacenamiento; puede ser uno de los siguientes: `read`, `pread`, `mmap`. El método `mmap` no se aplica a clickhouse-server (está pensado para clickhouse-local). Valor predeterminado: `pread` para clickhouse-server, `mmap` para clickhouse-local.