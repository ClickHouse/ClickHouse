---
description: 'Este motor permite integrar ClickHouse con Redis.'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Motor de tabla de Redis'
doc_type: 'guide'
---

Este motor permite integrar ClickHouse con [Redis](https://redis.io/). Dado que Redis utiliza un modelo clave-valor, recomendamos encarecidamente consultarlo solo mediante búsquedas puntuales, como `where k=xx` o `where k in (xx, xx)`.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**Parámetros del motor**

* `host:port` — dirección del servidor Redis; puede omitir el puerto y se usará el puerto predeterminado de Redis, 6379.
* `db_index` — índice de la base de datos de Redis, entre 0 y 15; el valor predeterminado es 0.
* `password` — contraseña del usuario; el valor predeterminado es una cadena vacía.
* `pool_size` — tamaño máximo del grupo de conexiones de Redis; el valor predeterminado es 16.
* `primary_key_name` - cualquier nombre de columna de la lista de columnas.

:::note Serialización
`PRIMARY KEY` solo admite una columna. La clave primaria se serializará en binario como una clave de Redis.
Las columnas distintas de la clave primaria se serializarán en binario como un valor de Redis en el orden correspondiente.
:::

Los argumentos también pueden pasarse mediante [colecciones con nombre](/es/operations/named-collections.md). En este caso, `host` y `port` deben especificarse por separado. Este enfoque se recomienda para entornos de producción. En este momento, todos los parámetros pasados a Redis mediante colecciones con nombre son obligatorios.

:::note Filtrado
Las consultas con `key equals` o con filtrado `in` se optimizarán como búsquedas de múltiples claves en Redis. Si las consultas no incluyen una clave de filtrado, se realizará un escaneo completo de la tabla, que es una operación costosa.
:::

<div id="usage-example">
  ## Ejemplo de uso
</div>

Cree una tabla en ClickHouse con el motor `Redis` usando argumentos básicos:

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

O bien usando [colecciones con nombre](/es/operations/named-collections.md):

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

Inserción:

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

Actualización:

Tenga en cuenta que la clave primaria no puede actualizarse.

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

Eliminar:

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate:

Vacía la base de datos de Redis de forma asíncrona. `Truncate` también admite el modo SYNC.

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

Join:

JOIN con otras tablas.

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## Limitaciones
</div>

El motor Redis también admite consultas de escaneo, como `where k > xx`, pero tiene algunas limitaciones:

1. La consulta de escaneo puede producir algunas claves duplicadas en casos muy poco frecuentes durante un rehash. Consulta los detalles en [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269).
2. Durante el escaneo, se pueden crear y eliminar claves, por lo que el conjunto de datos resultante no puede representar un instante válido en el tiempo.