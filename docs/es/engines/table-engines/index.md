---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Motores de mesa
toc_priority: 26
toc_title: "Implantaci\xF3n"
---

# Motores de mesa {#table_engines}

El motor de tabla (tipo de tabla) determina:

-   Cómo y dónde se almacenan los datos, dónde escribirlos y dónde leerlos.
-   Qué consultas son compatibles y cómo.
-   Acceso a datos simultáneos.
-   Uso de índices, si está presente.
-   Si es posible la ejecución de solicitudes multiproceso.
-   Parámetros de replicación de datos.

## Familias de motores {#engine-families}

### Método de codificación de datos: {#mergetree}

Los motores de mesa más universales y funcionales para tareas de alta carga. La propiedad compartida por estos motores es la inserción rápida de datos con el posterior procesamiento de datos en segundo plano. `MergeTree` Los motores familiares admiten la replicación de datos (con [Replicado\*](mergetree-family/replication.md#table_engines-replication) versiones de motores), particionamiento y otras características no admitidas en otros motores.

Motores en la familia:

-   [Método de codificación de datos:](mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [AgregaciónMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [ColapsarMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md#graphitemergetree)

### Registro {#log}

Ligero [motor](log-family/index.md) con funcionalidad mínima. Son los más efectivos cuando necesita escribir rápidamente muchas tablas pequeñas (hasta aproximadamente 1 millón de filas) y leerlas más tarde como un todo.

Motores en la familia:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [StripeLog](log-family/stripelog.md#stripelog)
-   [Registro](log-family/log.md#log)

### Motores de integración {#integration-engines}

Motores para comunicarse con otros sistemas de almacenamiento y procesamiento de datos.

Motores en la familia:

-   [Kafka](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [HDFS](integrations/hdfs.md#hdfs)

### Motores especiales {#special-engines}

Motores en la familia:

-   [Distribuido](special/distributed.md#distributed)
-   [Método de codificación de datos:](special/materializedview.md#materializedview)
-   [Diccionario](special/dictionary.md#dictionary)
-   \[Fusión\](special/merge.md#merge
-   [File](special/file.md#file)
-   [Nulo](special/null.md#null)
-   [Establecer](special/set.md#set)
-   [Unir](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [Vista](special/view.md#table_engines-view)
-   [Memoria](special/memory.md#memory)
-   [Búfer](special/buffer.md#buffer)

## Virtual Columnas {#table_engines-virtual_columns}

La columna virtual es un atributo de motor de tabla integral que se define en el código fuente del motor.

No debe especificar columnas virtuales en el `CREATE TABLE` consulta y no puedes verlos en `SHOW CREATE TABLE` y `DESCRIBE TABLE` resultados de la consulta. Las columnas virtuales también son de solo lectura, por lo que no puede insertar datos en columnas virtuales.

Para seleccionar datos de una columna virtual, debe especificar su nombre en el `SELECT` consulta. `SELECT *` no devuelve valores de columnas virtuales.

Si crea una tabla con una columna que tiene el mismo nombre que una de las columnas virtuales de la tabla, la columna virtual se vuelve inaccesible. No recomendamos hacer esto. Para ayudar a evitar conflictos, los nombres de columna virtual suelen tener el prefijo de un guión bajo.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
