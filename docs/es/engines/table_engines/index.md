---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_folder_title: Table Engines
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

### Mergetree {#mergetree}

Los motores de mesa más universales y funcionales para tareas de alta carga. La propiedad compartida por estos motores es la inserción rápida de datos con el posterior procesamiento de datos en segundo plano. `MergeTree` Los motores familiares admiten la replicación de datos (con [Replicado\*](mergetree_family/replication.md) versiones de motores), particionamiento y otras características no admitidas en otros motores.

Motores en la familia:

-   [Método de codificación de datos:](mergetree_family/mergetree.md)
-   [ReplacingMergeTree](mergetree_family/replacingmergetree.md)
-   [SummingMergeTree](mergetree_family/summingmergetree.md)
-   [AgregaciónMergeTree](mergetree_family/aggregatingmergetree.md)
-   [ColapsarMergeTree](mergetree_family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree_family/versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](mergetree_family/graphitemergetree.md)

### Registro {#log}

Ligero [motor](log_family/index.md) con funcionalidad mínima. Son los más efectivos cuando necesita escribir rápidamente muchas tablas pequeñas (hasta aproximadamente 1 millón de filas) y leerlas más tarde como un todo.

Motores en la familia:

-   [TinyLog](log_family/tinylog.md)
-   [StripeLog](log_family/stripelog.md)
-   [Registro](log_family/log.md)

### Motores de integración {#integration-engines}

Motores para comunicarse con otros sistemas de almacenamiento y procesamiento de datos.

Motores en la familia:

-   [Kafka](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)
-   [HDFS](integrations/hdfs.md)

### Motores especiales {#special-engines}

Motores en la familia:

-   [Distribuido](special/distributed.md)
-   [Método de codificación de datos:](special/materializedview.md)
-   [Diccionario](special/dictionary.md)
-   [Fusionar](special/merge.md)
-   [File](special/file.md)
-   [Nulo](special/null.md)
-   [Establecer](special/set.md)
-   [Unir](special/join.md)
-   [URL](special/url.md)
-   [Vista](special/view.md)
-   [Memoria](special/memory.md)
-   [Búfer](special/buffer.md)

## Virtual Columnas {#table_engines-virtual-columns}

La columna virtual es un atributo de motor de tabla integral que se define en el código fuente del motor.

No debe especificar columnas virtuales en el `CREATE TABLE` consulta y no puedes verlos en `SHOW CREATE TABLE` y `DESCRIBE TABLE` resultados de la consulta. Las columnas virtuales también son de solo lectura, por lo que no puede insertar datos en columnas virtuales.

Para seleccionar datos de una columna virtual, debe especificar su nombre en el `SELECT` consulta. `SELECT *` no devuelve valores de columnas virtuales.

Si crea una tabla con una columna que tiene el mismo nombre que una de las columnas virtuales de la tabla, la columna virtual se vuelve inaccesible. No recomendamos hacer esto. Para ayudar a evitar conflictos, los nombres de columna virtual suelen tener el prefijo de un guión bajo.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/) <!--hide-->
