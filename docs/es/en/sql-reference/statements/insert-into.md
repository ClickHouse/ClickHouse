---
description: 'Documentación de la sentencia INSERT INTO'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'Sentencia INSERT INTO'
doc_type: 'reference'
---

Inserta datos en una tabla.

**Sintaxis**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Puede especificar una lista de columnas para insertar usando `(c1, c2, c3)`. También puede usar una expresión con el [selector](../../sql-reference/statements/select/index.md#asterisk) de columnas, como `*`, y/o [modificadores](../../sql-reference/statements/select/index.md#select-modifiers) como [APPLY](/es/sql-reference/statements/select/apply-modifier), [EXCEPT](/es/sql-reference/statements/select/except-modifier), [REPLACE](/es/sql-reference/statements/select/replace-modifier).

Por ejemplo, considere la tabla:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

Si desea insertar datos en todas las columnas, excepto la columna `b`, puede hacerlo con la palabra clave `EXCEPT`. De acuerdo con la sintaxis anterior, deberá asegurarse de insertar tantos valores (`VALUES (v11, v13)`) como columnas especifique (`(c1, c3)`) :

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

En este ejemplo, vemos que la segunda fila insertada tiene las columnas `a` y `c` rellenadas con los valores proporcionados, y `b` con el valor predeterminado. También es posible usar la palabra clave `DEFAULT` para insertar valores predeterminados:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

Si una lista de columnas no incluye todas las columnas existentes, las demás columnas se rellenan con:

* Los valores calculados a partir de las expresiones `DEFAULT` especificadas en la definición de la tabla.
* Ceros y cadenas vacías, si no se han definido expresiones `DEFAULT`.

Los datos pueden pasarse a `INSERT` en cualquier [formato](/es/sql-reference/formats) compatible con ClickHouse. El formato debe especificarse explícitamente en la consulta:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

Por ejemplo, el siguiente formato de consulta es idéntico a la versión básica de `INSERT ... VALUES`:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse elimina todos los espacios y un salto de línea (si lo hay) antes de los datos. Al construir una consulta, recomendamos colocar los datos en una línea nueva después de los operadores de la consulta, lo cual es importante si los datos empiezan con espacios.

Ejemplo:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Puede insertar datos por separado de la consulta mediante el [cliente de línea de comandos](/es/operations/utilities/clickhouse-local) o la [interfaz HTTP](/es/interfaces/http).

:::note
Si quiere especificar `SETTINGS` para la consulta `INSERT`, debe hacerlo *antes* de la cláusula `FORMAT`, ya que todo lo que aparece después de `FORMAT format_name` se trata como datos. Por ejemplo:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## Restricciones
</div>

Si una tabla tiene [restricciones](../../sql-reference/statements/create/table.md#constraints), sus expresiones se comprobarán en cada fila de los datos insertados. Si alguna de esas restricciones no se cumple, el servidor generará una excepción que incluirá el nombre y la expresión de la restricción, y la consulta se detendrá.

<div id="data-type-validation">
  ## Validación de tipos de datos
</div>

ClickHouse valida los tipos de datos permitidos (controlados por ajustes como `enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types`, etc.) solo durante la creación de tablas (`CREATE TABLE`) y la modificación del esquema (`ALTER TABLE`), no durante `INSERT`.

Esto significa que, si ya existe una tabla con un tipo de datos no permitido, se pueden insertar datos en ella incluso cuando el ajuste correspondiente está deshabilitado en el servidor. Esto es intencional: una vez creada una tabla, las inserciones no deben quedar bloqueadas por ajustes que controlan la creación de tipos.

Por ejemplo:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
Como consecuencia, un client con una versión más reciente (en la que una configuración está habilitada de forma predeterminada) puede insertar datos con tipos de datos no permitidos en un servidor con una versión anterior (en la que la configuración está deshabilitada), siempre que la tabla de destino ya tenga los tipos de columna correspondientes. La validación se aplica a nivel de DDL, no a nivel de DML.
:::

<div id="inserting-the-results-of-select">
  ## Inserción de los resultados de SELECT
</div>

**Sintaxis**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

Las columnas se asignan según su posición en la cláusula `SELECT`. Sin embargo, sus nombres en la expresión `SELECT` y en la tabla de `INSERT` pueden diferir. Si es necesario, se realiza una conversión de tipos.

Ninguno de los formatos de datos, excepto el formato Values, permite asignar valores a expresiones como `now()`, `1 + 2`, etc. El formato Values permite un uso limitado de expresiones, pero no se recomienda, porque en ese caso se utiliza código ineficiente para ejecutarlas.

No se admiten otras consultas para modificar partes de datos: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Sin embargo, puede eliminar datos antiguos con `ALTER TABLE ... DROP PARTITION`.

La cláusula `FORMAT` debe especificarse al final de la consulta si la cláusula `SELECT` contiene la función de tabla [input()](../../sql-reference/table-functions/input.md).

Para insertar un valor predeterminado en lugar de `NULL` en una columna con un tipo de dato no anulable, habilite la configuración [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default).

`INSERT` también admite CTE (common table expression). Por ejemplo, las dos sentencias siguientes son equivalentes:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## Insertar datos desde un archivo
</div>

**Sintaxis**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Usa la sintaxis anterior para insertar datos desde un archivo, o varios archivos, almacenados en el **cliente**. `file_name` y `type` son literales de cadena. El [formato](../../interfaces/formats.md) del archivo de entrada debe especificarse en la cláusula `FORMAT`.

Se admiten archivos comprimidos. El tipo de compresión se detecta a partir de la extensión del nombre del archivo. También puede especificarse explícitamente en una cláusula `COMPRESSION`. Los tipos admitidos son: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

Esta funcionalidad está disponible en el [cliente de línea de comandos](../../interfaces/client.md) y en [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Ejemplos**

<div id="single-file-with-from-infile">
  ### Un solo archivo con FROM INFILE
</div>

Ejecute las siguientes consultas con el [cliente de línea de comandos](../../interfaces/client.md):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### Varios archivos con FROM INFILE usando globs
</div>

Este ejemplo es muy similar al anterior, pero las inserciones se hacen desde varios archivos mediante `FROM INFILE 'input_*.csv`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
Además de seleccionar varios archivos con `*`, puedes usar rangos (`{1,2}` o `{1..9}`) y otras [sustituciones de glob](/es/sql-reference/table-functions/file.md/#globs-in-path). Las tres siguientes funcionarían con el ejemplo anterior:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## Inserción mediante una función de tabla
</div>

Los datos se pueden insertar en las tablas referenciadas por las [funciones de tabla](../../sql-reference/table-functions/index.md).

**Sintaxis**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Ejemplo**

La función de tabla [remote](/es/sql-reference/table-functions/remote) se utiliza en las siguientes consultas:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## Inserción en ClickHouse Cloud
</div>

De forma predeterminada, los servicios de ClickHouse Cloud ofrecen varias réplicas para garantizar una alta disponibilidad. Cuando se conecta a un servicio, la conexión se establece con una de estas réplicas.

Una vez que un `INSERT` se completa correctamente, los datos se escriben en el almacenamiento subyacente. Sin embargo, las réplicas pueden tardar un tiempo en recibir estas actualizaciones. Por lo tanto, si utiliza una conexión diferente que ejecuta una consulta `SELECT` en alguna de esas otras réplicas, es posible que los datos actualizados aún no se reflejen.

Puede usar `select_sequential_consistency` para forzar a la réplica a recibir las actualizaciones más recientes. A continuación, se muestra un ejemplo de una consulta `SELECT` que utiliza esta configuración:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Tenga en cuenta que usar `select_sequential_consistency` aumentará la carga de ClickHouse Keeper (usado internamente por ClickHouse Cloud) y puede provocar un rendimiento más lento según la carga del servicio. Recomendamos no habilitar esta configuración a menos que sea necesario. El enfoque recomendado es ejecutar las operaciones de lectura y escritura en la misma sesión o usar un driver de cliente que utilice el protocolo nativo (y, por lo tanto, admita conexiones sticky).

<div id="inserting-into-a-replicated-setup">
  ## Inserción en una configuración replicada
</div>

En una configuración replicada, los datos serán visibles en otras réplicas una vez que se hayan replicado. Los datos empiezan a replicarse (a descargarse en otras réplicas) inmediatamente después de un `INSERT`. Esto difiere de ClickHouse Cloud, donde los datos se escriben de inmediato en el almacenamiento compartido y las réplicas se suscriben a los cambios de metadatos.

Ten en cuenta que, en las configuraciones replicadas, los `INSERTs` a veces pueden tardar bastante tiempo (del orden de un segundo), ya que requieren hacer commit en ClickHouse Keeper para alcanzar consenso distribuido. El uso de S3 como almacenamiento también añade latencia.

<div id="performance-considerations">
  ## Consideraciones de rendimiento
</div>

`INSERT` ordena los datos de entrada por clave primaria y los divide en particiones según una clave de partición. Si se insertan datos en varias particiones a la vez, el rendimiento de la consulta `INSERT` puede reducirse significativamente. Para evitarlo:

* Añada los datos en lotes bastante grandes, por ejemplo, 100.000 filas cada vez.
* Agrupe los datos por clave de partición antes de cargarlos en ClickHouse.

El rendimiento no disminuirá si:

* Los datos se añaden en tiempo real.
* Se cargan datos que normalmente están ordenados por tiempo.

<div id="asynchronous-inserts">
  ### Inserciones asíncronas
</div>

Es posible insertar datos de forma asíncrona mediante inserciones pequeñas pero frecuentes. Los datos de esas inserciones se agrupan en lotes y luego se insertan de forma segura en una tabla. Para usar las inserciones asíncronas, habilite la opción de configuración [`async_insert`](/es/operations/settings/settings#async_insert).

Usar `async_insert` o el [motor de tabla `Buffer`](/es/engines/table-engines/special/buffer) implica un almacenamiento en búfer adicional.

<div id="large-or-long-running-inserts">
  ### Inserciones grandes o de larga duración
</div>

Cuando se insertan grandes cantidades de datos, ClickHouse optimiza el rendimiento de escritura mediante un proceso llamado &quot;squashing&quot;. Los bloques pequeños de datos insertados en memoria se fusionan y se agrupan en bloques más grandes antes de escribirse en disco. El &quot;squashing&quot; reduce la sobrecarga asociada a cada operación de escritura. En este proceso, los datos insertados estarán disponibles para consulta después de que ClickHouse termine de escribir cada bloque de [`max_insert_block_size`](/es/operations/settings/settings#max_insert_block_size) filas.

**Véase también**

* [async&#95;insert](/es/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/es/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/es/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/es/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/es/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/es/operations/settings/settings#async_insert_max_data_size)