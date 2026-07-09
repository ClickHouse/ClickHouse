---
description: 'Este motor permite integrar ClickHouse con RocksDB'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'Motor de tabla EmbeddedRocksDB'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # Motor de tabla EmbeddedRocksDB
</div>

<CloudNotSupportedBadge />

Este motor permite integrar ClickHouse con [RocksDB](http://rocksdb.org/).

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

Parámetros del motor:

* `ttl` - tiempo de vida de los valores. TTL se expresa en segundos. Si TTL es 0, se usa una instancia normal de RocksDB (sin TTL).
* `rocksdb_dir` - ruta al directorio de un RocksDB existente o ruta de destino del RocksDB que se va a crear. Abre la tabla con el `rocksdb_dir` especificado.
* `read_only` - cuando `read_only` se establece en true, se usa el modo de solo lectura. En el almacenamiento con TTL, la compactación no se activará (ni manual ni automáticamente), por lo que no se eliminarán las entradas caducadas.
* `primary_key_name` – cualquier nombre de columna de la lista de columnas.
* `primary key` debe especificarse; solo admite una columna en la clave primaria. La clave primaria se serializará en binario como una `rocksdb key`.
* las columnas distintas de la clave primaria se serializarán en binario como valor `rocksdb` en el orden correspondiente.
* las consultas con filtrado por clave `equals` o `in` se optimizarán como una búsqueda de varias claves en `rocksdb`.

Configuración del motor:

* `optimize_for_bulk_insert` – La tabla está optimizada para inserciones masivas (el pipeline de inserción creará archivos SST y los importará a la base de datos de rocksdb en lugar de escribir en memtables); valor predeterminado: `1`.
* `bulk_insert_block_size` - tamaño mínimo de los archivos SST (en número de filas) creados por la inserción masiva; valor predeterminado: `1048449`.

Ejemplo:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

<div id="metrics">
  ## Métricas
</div>

También está la tabla `system.rocksdb`, que expone estadísticas de RocksDB:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

<div id="configuration">
  ## Configuración
</div>

También puedes cambiar cualquier [opción de RocksDB](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) mediante la configuración:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

De forma predeterminada, la optimización de recuento aproximado trivial está desactivada, lo que puede afectar al rendimiento de las consultas `count()`. Para habilitar esta
optimización, configure `optimize_trivial_approximate_count_query = 1`. Además, esta configuración también afecta a `system.tables` en el motor EmbeddedRocksDB;
actívela para ver valores aproximados de `total_rows` y `total_bytes`.

<div id="supported-operations">
  ## Operaciones compatibles
</div>

<div id="inserts">
  ### Inserciones
</div>

Al insertar nuevas filas en `EmbeddedRocksDB`, si la clave ya existe, se actualizará el valor; de lo contrario, se creará una nueva clave.

Ejemplo:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Borrado
</div>

Las filas pueden eliminarse con la consulta `DELETE` o con `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### Actualizaciones
</div>

Los valores pueden actualizarse con la consulta `ALTER TABLE`. La clave primaria no puede actualizarse.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### Joins
</div>

Se admite un `direct` join especial con tablas EmbeddedRocksDB.
Este direct join evita crear una tabla hash en memoria y accede
a los datos directamente desde EmbeddedRocksDB.

Con joins grandes, es posible que el uso de memoria sea mucho menor con direct joins
porque no se crea la tabla hash.

Para habilitar los direct joins:

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
Cuando `join_algorithm` está configurado como `direct, hash`, se utilizarán direct joins
siempre que sea posible, y hash en caso contrario.
:::

<div id="example">
  #### Ejemplo
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### Crear y poblar una tabla EmbeddedRocksDB
</div>

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### Crear y poblar una tabla para hacer join con la tabla `rdb`
</div>

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

<div id="set-the-join-algorithm-to-direct">
  ##### Configura el algoritmo de join como `direct`
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### Un INNER JOIN
</div>

```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```

```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

<div id="more-information-on-joins">
  ### Más información sobre JOIN
</div>

* [ajuste `join_algorithm`](/es/operations/settings/settings.md#join_algorithm)
* [cláusula JOIN](/es/sql-reference/statements/select/join.md)