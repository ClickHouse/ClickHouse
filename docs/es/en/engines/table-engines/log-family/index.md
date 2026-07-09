---
description: 'Documentación de la familia de motores Log'
sidebar_label: 'Familia Log'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Familia de motores Log'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Familia de motores de tablas Log
</div>

<CloudNotSupportedBadge />

Estos motores se desarrollaron para casos en los que necesitas escribir rápidamente muchas tablas pequeñas (de hasta aproximadamente 1 millón de filas) y leerlas después como un todo.

Motores de la familia:

| Motores Log                                                 |
| ----------------------------------------------------------- |
| [StripeLog](/es/engines/table-engines/log-family/stripelog.md) |
| [Log](/es/engines/table-engines/log-family/log.md)             |
| [TinyLog](/es/engines/table-engines/log-family/tinylog.md)     |

Los motores de tablas de la familia `Log` pueden almacenar datos en los sistemas de archivos distribuidos [HDFS](/es/engines/table-engines/integrations/hdfs) o [S3](/es/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).

:::warning Este motor no está pensado para datos de logs.
A pesar del nombre, *los motores de tablas Log no están pensados para almacenar datos de logs. Solo deben usarse con volúmenes pequeños que deban escribirse rápidamente.
:::

<div id="common-properties">
  ## Propiedades comunes
</div>

Motores:

* Almacenan los datos en disco.

* Añaden datos al final del archivo al escribir.

* Admiten bloqueos para el acceso concurrente a los datos.

  Durante las consultas `INSERT`, la tabla se bloquea y el resto de consultas, tanto de lectura como de escritura, deben esperar a que se desbloquee. Si no hay consultas de escritura, puede ejecutarse de forma concurrente cualquier número de consultas de lectura.

* No admiten [mutaciones](/es/sql-reference/statements/alter#mutations).

* No admiten índices.

  Esto significa que las consultas `SELECT` sobre rangos de datos no son eficientes.

* No escriben los datos de forma atómica.

  La tabla puede quedar con datos corruptos si algo interrumpe la operación de escritura, por ejemplo, un apagado anómalo del servidor.

<div id="differences">
  ## Diferencias
</div>

El motor `TinyLog` es el más simple de la familia y ofrece la funcionalidad más limitada y la menor eficiencia. El motor `TinyLog` no admite la lectura paralela de datos mediante varios hilos en una sola consulta. Lee los datos más lentamente que otros motores de la familia que admiten lectura paralela en una sola consulta y utiliza casi tantos descriptores de archivo como el motor `Log`, ya que almacena cada columna en un archivo independiente. Úselo solo en escenarios simples.

Los motores `Log` y `StripeLog` admiten la lectura paralela de datos. Al leer datos, ClickHouse utiliza varios hilos. Cada hilo procesa un bloque de datos independiente. El motor `Log` utiliza un archivo independiente para cada columna de la tabla. `StripeLog` almacena todos los datos en un único archivo. Como resultado, el motor `StripeLog` utiliza menos descriptores de archivo, pero el motor `Log` ofrece una mayor eficiencia al leer datos.