---
description: 'Este motor le permite usar el clúster de Keeper/ZooKeeper como un almacén de clave-valor consistente, con escrituras linealizables y lecturas con consistencia secuencial.'
sidebar_label: 'KeeperMap'
sidebar_position: 150
slug: /engines/table-engines/special/keeper-map
title: 'Motor de tabla KeeperMap'
doc_type: 'referencia'
---

Este motor le permite usar el clúster de Keeper/ZooKeeper como un almacén de clave-valor consistente, con escrituras linealizables y lecturas con consistencia secuencial.

Para habilitar el motor de almacenamiento KeeperMap, debe definir una ruta de ZooKeeper donde se almacenarán las tablas mediante la configuración `<keeper_map_path_prefix>`.

Por ejemplo:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

donde la ruta puede ser cualquier otra ruta válida de ZooKeeper.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

Parámetros del motor:

* `root_path` - ruta de ZooKeeper donde se almacenará `table_name`.
  Esta ruta no debe contener el prefijo definido en la configuración `<keeper_map_path_prefix>`, ya que el prefijo se añadirá automáticamente a `root_path`.
  Además, también se admite el formato `auxiliary_zookeeper_cluster_name:/some/path`, donde `auxiliary_zookeeper_cluster` es un clúster de ZooKeeper definido dentro de la configuración `<auxiliary_zookeepers>`.
  De forma predeterminada, se utiliza el clúster de ZooKeeper definido dentro de la configuración `<zookeeper>`.
* `keys_limit` - número de claves permitidas dentro de la tabla.
  Este límite es flexible y, en algunos casos extremos, es posible que la tabla acabe conteniendo más claves de las permitidas.
* `primary_key_name` – cualquier nombre de columna de la lista de columnas.
* se debe especificar la `primary key`; solo admite una columna en la clave primaria. La clave primaria se serializará en binario como un `nombre de nodo` dentro de ZooKeeper.
* las columnas distintas de la clave primaria se serializarán en binario en el orden correspondiente y se almacenarán como valor del nodo resultante definido por la clave serializada.
* las consultas con filtrado por clave `equals` o `in` se optimizarán para realizar una búsqueda de varias claves en `Keeper`; en caso contrario, se recuperarán todos los valores.

Ejemplo:

```sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

con

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

Cada valor, que es la serialización binaria de `(v1, v2, v3)`, se almacenará en `/keeper_map_tables/keeper_map_table/data/serialized_key` en `Keeper`.
Además, el número de claves tendrá un límite no estricto de 4.

Si se crean varias tablas en la misma ruta de ZooKeeper, los valores se conservan mientras exista al menos 1 tabla que la utilice.
Como resultado, es posible usar la cláusula `ON CLUSTER` al crear la tabla y compartir los datos entre varias instancias de ClickHouse.
Por supuesto, también es posible ejecutar manualmente `CREATE TABLE` con la misma ruta en instancias de ClickHouse no relacionadas para lograr el mismo efecto de uso compartido de datos.

<div id="supported-operations">
  ## Operaciones compatibles
</div>

<div id="inserts">
  ### Inserciones
</div>

Cuando se insertan nuevas filas en `KeeperMap`, si la clave no existe, se crea una nueva entrada para esa clave.
Si la clave existe y la configuración `keeper_map_strict_mode` está establecida en `true`, se genera una excepción; de lo contrario, se sobrescribe el valor de la clave.

Ejemplo:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Borrado
</div>

Las filas se pueden eliminar con la consulta `DELETE` o con `TRUNCATE`.
Si la clave existe y la configuración `keeper_map_strict_mode` está establecida en `true`, la recuperación y la eliminación de datos solo tendrán éxito si pueden ejecutarse de forma atómica.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

<div id="updates">
  ### Actualizaciones
</div>

Los valores pueden actualizarse mediante la consulta `ALTER TABLE`. La clave primaria no se puede actualizar.
Si la configuración `keeper_map_strict_mode` está establecida en `true`, la obtención y actualización de datos solo tendrán éxito si se ejecutan de forma atómica.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo crear aplicaciones de analítica en tiempo real con ClickHouse y Hex](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)