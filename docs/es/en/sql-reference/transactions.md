---
description: 'Página que describe el soporte transaccional (ACID) en ClickHouse'
slug: /guides/developer/transactional
title: 'Soporte transaccional (ACID)'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # soporte transaccional
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## Caso 1: INSERT en una partición de una tabla de la familia MergeTree*
</div>

Esto es transaccional (ACID) si las filas insertadas se empaquetan y se insertan como un solo bloque (consulte las Notas):

* Atómico: un INSERT se completa correctamente o se rechaza en su totalidad: si se envía una confirmación al Client, se insertaron todas las filas; si se envía un error al Client, no se insertó ninguna fila.
* Consistente: si no se viola ninguna restricción de la tabla, se insertan todas las filas de un INSERT y el INSERT se completa correctamente; si se violan restricciones, no se inserta ninguna fila.
* Aislado: los Clients concurrentes observan una instantánea coherente de la tabla: el estado de la tabla tal como era antes del intento de INSERT o después de que el INSERT se completara correctamente; no se observa ningún estado parcial. Los Clients dentro de otra transacción tienen [aislamiento de instantánea](https://en.wikipedia.org/wiki/Snapshot_isolation), mientras que los Clients fuera de una transacción tienen el nivel de aislamiento [lectura no confirmada](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted).
* Duradero: un INSERT exitoso se escribe en el sistema de archivos antes de responder al Client, en una sola réplica o en varias réplicas (controlado por la configuración `insert_quorum`), y ClickHouse puede pedirle al SO que sincronice los datos del sistema de archivos con el medio de almacenamiento (controlado por la configuración `fsync_after_insert`).
* Es posible hacer INSERT en varias tablas con una sola instrucción si intervienen vistas materializadas (el INSERT del Client se realiza en una tabla que tiene vistas materializadas asociadas).

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## Caso 2: INSERT en múltiples particiones de una tabla de la familia MergeTree*
</div>

Igual que el caso 1 anterior, con este detalle:

* Si la tabla tiene muchas particiones y el INSERT abarca muchas particiones, la inserción en cada partición es transaccional de forma independiente

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## Caso 3: INSERT en una tabla distribuida de la familia MergeTree*
</div>

Igual que en el caso 1 anterior, con este detalle:

* El INSERT en la tabla Distributed no es transaccional en su conjunto, mientras que la inserción en cada segmento sí lo es

<div id="case-4-using-a-buffer-table">
  ## Caso 4: Uso de una tabla Buffer
</div>

* las inserciones en tablas Buffer no son atómicas, ni aisladas, ni consistentes, ni duraderas

<div id="case-5-using-async_insert">
  ## Caso 5: Uso de async_insert
</div>

Igual que el caso 1 anterior, con este detalle:

* la atomicidad está garantizada incluso si `async_insert` está habilitado y `wait_for_async_insert` está establecido en 1 (valor predeterminado), pero si `wait_for_async_insert` está establecido en 0, la atomicidad no está garantizada.

<div id="notes">
  ## Notas
</div>

* las filas insertadas desde el Client en algún formato de datos se empaquetan en un único bloque cuando:
  * el formato de inserción se basa en filas (como CSV, TSV, Values, JSONEachRow, etc.) y los datos contienen menos de `max_insert_block_size` filas (~1 000 000 de forma predeterminada) o menos de `min_chunk_bytes_for_parallel_parsing` bytes (10 MB de forma predeterminada) en caso de que se utilice el análisis en paralelo (habilitado de forma predeterminada)
  * el formato de inserción se basa en columnas (como Native, Parquet, ORC, etc.) y los datos contienen un solo bloque de datos
* el tamaño del bloque insertado, en general, puede depender de muchos parámetros (por ejemplo: `max_block_size`, `max_insert_block_size`, `min_insert_block_size_rows`, `min_insert_block_size_bytes`, `preferred_block_size_bytes`, etc.)
* si el Client no recibió respuesta del server, el Client no sabe si la transacción se realizó correctamente y puede repetirla usando propiedades de inserción exactly-once
* ClickHouse usa internamente [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) con [aislamiento de instantánea](https://en.wikipedia.org/wiki/Snapshot_isolation) para transacciones concurrentes
* todas las propiedades ACID son válidas incluso en caso de kill/crash del server
* se debe habilitar insert&#95;quorum en diferentes AZ o fsync para garantizar inserciones duraderas en una configuración típica
* la &quot;consistencia&quot; en términos de ACID no cubre la semántica de los sistemas distribuidos; consulta https://jepsen.io/consistency, que se controla mediante diferentes parámetros (select&#95;sequential&#95;consistency)
* esta explicación no cubre una nueva funcionalidad de transacciones que permite tener transacciones completas sobre múltiples tablas, vistas materializadas, múltiples SELECT, etc. (consulta la siguiente sección sobre Transactions, Commit, and Rollback)

<div id="transactions-commit-and-rollback">
  ## Transacciones, commit y rollback
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Además de la funcionalidad descrita al inicio de este documento, ClickHouse ofrece soporte experimental para transacciones, commits y operaciones de rollback.

<div id="requirements">
  ### Requisitos
</div>

* Despliegue ClickHouse Keeper o ZooKeeper para realizar el seguimiento de las transacciones
* Solo DB Atomic (predeterminada)
* Solo el table engine MergeTree no replicado
* Habilite la compatibilidad Experimental con transacciones añadiendo esta configuración en `config.d/transactions.xml`:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### Notas
</div>

* Esta es una funcionalidad experimental, por lo que se esperan cambios.
* Si se produce una excepción durante una transacción, no se puede hacer commit de la transacción. Esto incluye todas las excepciones, incluidas las excepciones `UNKNOWN_FUNCTION` causadas por errores tipográficos.
* Las transacciones anidadas no son compatibles; en su lugar, finalice la transacción actual e inicie una nueva

<div id="configuration">
  ### Configuración
</div>

Estos ejemplos usan un servidor ClickHouse de un solo nodo con ClickHouse Keeper habilitado.

<div id="enable-experimental-transaction-support">
  #### Habilitar la compatibilidad experimental con transacciones
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### Configuración básica para un único nodo del servidor ClickHouse con ClickHouse Keeper habilitado
</div>

:::note
Consulta la documentación sobre [implementación](/es/deployment-guides/terminology.md) para obtener más información sobre cómo desplegar el servidor ClickHouse y un quórum adecuado de nodos de ClickHouse Keeper. La configuración que se muestra aquí es solo con fines experimentales.
:::

```xml title=/etc/clickhouse-server/config.d/config.xml
<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <display_name>node 1</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <zookeeper>
        <node>
            <host>clickhouse-01</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse-keeper-01</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

<div id="example">
  ### Ejemplo
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### Verifique que las transacciones experimentales estén habilitadas
</div>

Ejecute un `BEGIN TRANSACTION` o `START TRANSACTION`, seguido de un `ROLLBACK`, para verificar que las transacciones experimentales estén habilitadas y que ClickHouse Keeper también lo esté, ya que se utiliza para hacer el seguimiento de las transacciones.

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
Si aparece el siguiente error, comprueba el archivo de configuración para asegurarte de que `allow_experimental_transactions` esté establecido en `1` (o en cualquier valor distinto de `0` o `false`).

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

También puedes verificar ClickHouse Keeper ejecutando

```bash
echo ruok | nc localhost 9181
```

ClickHouse Keeper debe responder con `imok`.
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### Cree una tabla para hacer pruebas
</div>

:::tip
La creación de tablas no es transaccional. Ejecute esta consulta DDL fuera de una transacción.
:::

```sql
CREATE TABLE mergetree_table
(
    `n` Int64
)
ENGINE = MergeTree
ORDER BY n
```

```response
Ok.
```

<div id="begin-a-transaction-and-insert-a-row">
  #### Iniciar una transacción e insertar una fila
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (10)
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 10 │
└────┘
```

:::note
Puedes consultar la tabla dentro de una transacción y ver que la fila se insertó aunque aún no se haya ejecutado el commit.
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### Revierte la transacción y vuelve a consultar la tabla
</div>

Verifica que la transacción se haya revertido:

```sql
ROLLBACK
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

<div id="complete-a-transaction-and-query-the-table-again">
  #### Completa una transacción y vuelve a consultar la tabla
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (42)
```

```response
Ok.
```

```sql
COMMIT
```

```response
Ok. Elapsed: 0.002 sec.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 42 │
└────┘
```

<div id="transactions-introspection">
  ### Inspección de transacciones
</div>

Puede inspeccionar las transacciones consultando la tabla `system.transactions`, pero tenga en cuenta que no es posible consultar esa tabla desde una sesión que esté dentro de una transacción. Abra una segunda sesión de `clickhouse client` para consultar esa tabla.

```sql
SELECT *
FROM system.transactions
FORMAT Vertical
```

```response
Row 1:
──────
tid:         (33,61,'51e60bce-6b82-4732-9e1d-b40705ae9ab8')
tid_hash:    11240433987908122467
elapsed:     210.017820947
is_readonly: 1
state:       RUNNING
```

<div id="more-details">
  ## Más detalles
</div>

Consulta este [meta issue](https://github.com/ClickHouse/ClickHouse/issues/48794) para ver pruebas mucho más exhaustivas y mantenerte al día de los avances.