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

Los motores de mesa más universales y funcionales para tareas de alta carga. La propiedad compartida por estos motores es la inserción rápida de datos con el posterior procesamiento de datos en segundo plano. `MergeTree` Los motores familiares admiten la replicación de datos (con [Replicado\*](replication.md) versiones de motores), particionamiento y otras características no admitidas en otros motores.

Motores en la familia:

-   [Método de codificación de datos:](mergetree.md)
-   [ReplacingMergeTree](replacingmergetree.md)
-   [SummingMergeTree](summingmergetree.md)
-   [AgregaciónMergeTree](aggregatingmergetree.md)
-   [ColapsarMergeTree](collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](graphitemergetree.md)

### Registro {#log}

Ligero [motor](log_family.md) con funcionalidad mínima. Son los más efectivos cuando necesita escribir rápidamente muchas tablas pequeñas (hasta aproximadamente 1 millón de filas) y leerlas más tarde en su conjunto.

Motores en la familia:

-   [TinyLog](tinylog.md)
-   [StripeLog](stripelog.md)
-   [Registro](log.md)

### Motores de intergación {#intergation-engines}

Motores para comunicarse con otros sistemas de almacenamiento y procesamiento de datos.

Motores en la familia:

-   [Kafka](kafka.md)
-   [MySQL](mysql.md)
-   [ODBC](odbc.md)
-   [JDBC](jdbc.md)
-   [HDFS](hdfs.md)

### Motores especiales {#special-engines}

Motores en la familia:

-   [Distribuido](distributed.md)
-   [Método de codificación de datos:](materializedview.md)
-   [Diccionario](dictionary.md)
-   [Fusionar](merge.md)
-   [File](file.md)
-   [Nulo](null.md)
-   [Establecer](set.md)
-   [Unir](join.md)
-   [URL](url.md)
-   [Vista](view.md)
-   [Memoria](memory.md)
-   [Búfer](buffer.md)

## Columnas virtuales {#table_engines-virtual-columns}

La columna virtual es un atributo de motor de tabla integral que se define en el código fuente del motor.

No debe especificar columnas virtuales en el `CREATE TABLE` consulta y no se puede ver en `SHOW CREATE TABLE` y `DESCRIBE TABLE` resultados de la consulta. Las columnas virtuales también son de solo lectura, por lo que no puede insertar datos en columnas virtuales.

Para seleccionar datos de una columna virtual, debe especificar su nombre en el `SELECT` consulta. `SELECT *` no devuelve valores de columnas virtuales.

Si crea una tabla con una columna que tiene el mismo nombre que una de las columnas virtuales de la tabla, la columna virtual se vuelve inaccesible. No recomendamos hacer esto. Para ayudar a evitar conflictos, los nombres de columna virtual suelen tener el prefijo de un guión bajo.

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/) <!--hide-->
