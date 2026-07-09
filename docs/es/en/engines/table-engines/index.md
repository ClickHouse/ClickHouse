---
description: 'Documentación de los motores de tabla'
slug: /engines/table-engines/
toc_folder_title: 'Motores de tabla'
toc_priority: 26
toc_title: 'Introducción'
title: 'Motores de tabla'
doc_type: 'reference'
---

El motor de tabla (tipo de tabla) determina:

* Cómo y dónde se almacenan los datos, dónde escribirlos y desde dónde leerlos.
* Qué consultas admite y de qué forma.
* El acceso concurrente a los datos.
* El uso de índices, si los hay.
* Si es posible ejecutar solicitudes en varios hilos.
* Los parámetros de replicación de datos.

<div id="engine-families">
  ## Familias de motores
</div>

<div id="mergetree">
  ### MergeTree
</div>

Los motores de tabla más versátiles y funcionales para tareas de alta carga. La característica común de estos motores es la rápida inserción de datos, con su posterior procesamiento en segundo plano. Los motores de la familia `MergeTree` admiten la replicación de datos (con las versiones [Replicated*](/es/engines/table-engines/mergetree-family/replication) de los motores), el particionamiento, índices secundarios de omisión de datos y otras funciones ausentes en otros motores.

Motores de la familia:

| Motores de MergeTree                                                                                 |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/es/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/es/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/es/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/es/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/es/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/es/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/es/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/es/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

[motores](../../engines/table-engines/log-family/index.md) ligeros con funcionalidad mínima. Son más eficaces cuando necesitas crear rápidamente muchas tablas pequeñas (de hasta aproximadamente 1 millón de filas) y leerlas después completas.

Motores de la familia:

| Motores Log                                              |
| -------------------------------------------------------- |
| [TinyLog](/es/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/es/engines/table-engines/log-family/stripelog) |
| [Log](/es/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### Motores de integración
</div>

Motores para comunicarse con otros sistemas de almacenamiento y procesamiento de datos.

Motores de la familia:

| Motores de integración                                                          |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### Motores especiales
</div>

Motores de la familia:

| Motores especiales                                             |
| -------------------------------------------------------------- |
| [Distributed](/es/engines/table-engines/special/distributed)      |
| [Diccionario](/es/engines/table-engines/special/dictionary)       |
| [Merge](/es/engines/table-engines/special/merge)                  |
| [Executable](/es/engines/table-engines/special/executable)        |
| [File](/es/engines/table-engines/special/file)                    |
| [Null](/es/engines/table-engines/special/null)                    |
| [Set](/es/engines/table-engines/special/set)                      |
| [Join](/es/engines/table-engines/special/join)                    |
| [URL](/es/engines/table-engines/special/url)                      |
| [View](/es/engines/table-engines/special/view)                    |
| [Memory](/es/engines/table-engines/special/memory)                |
| [Búfer](/es/engines/table-engines/special/buffer)                 |
| [Datos externos](/es/engines/table-engines/special/external-data) |
| [GenerateRandom](/es/engines/table-engines/special/generate)      |
| [KeeperMap](/es/engines/table-engines/special/keeper-map)         |
| [FileLog](/es/engines/table-engines/special/filelog)              |

<div id="table_engines-virtual_columns">
  ## Columnas virtuales
</div>

Una columna virtual es un atributo inherente del motor de tabla definido en el código fuente del motor.

No debes especificar columnas virtuales en la consulta `CREATE TABLE`, y no puedes verlas en los resultados de las consultas `SHOW CREATE TABLE` y `DESCRIBE TABLE`. Las columnas virtuales también son de solo lectura, por lo que no puedes insertar datos en ellas.

Para seleccionar datos de una columna virtual, debes especificar su nombre en la consulta `SELECT`. `SELECT *` no devuelve valores de las columnas virtuales.

Si creas una tabla con una columna que tiene el mismo nombre que una de las columnas virtuales de la tabla, la columna virtual deja de ser accesible. No recomendamos hacerlo. Para ayudar a evitar conflictos, los nombres de las columnas virtuales suelen llevar un prefijo de guion bajo.

* `_table` — Contiene el nombre de la tabla de la que se leyeron los datos. Tipo: [String](../../sql-reference/data-types/string.md).

  Independientemente del motor de tabla que se utilice, cada tabla incluye una columna virtual universal llamada `_table`.

  Al consultar una tabla con el motor de tabla Merge, puedes establecer condiciones constantes sobre `_table` en la cláusula `WHERE/PREWHERE` (por ejemplo, `WHERE _table='xyz'`). En este caso, la operación de lectura se realiza solo para aquellas tablas en las que se cumple la condición sobre `_table`, por lo que la columna `_table` actúa como un índice.

  Al usar consultas con el formato `SELECT ... FROM (... UNION ALL ...)`, podemos determinar de qué tabla real proceden las filas devueltas especificando la columna `_table`.