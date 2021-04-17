---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "Clave de partici\xF3n personalizada"
---

# Clave de partición personalizada {#custom-partitioning-key}

La partición está disponible para el [Método de codificación de datos:](mergetree.md) mesas familiares (incluyendo [repetición](replication.md) tabla). [Vistas materializadas](../special/materializedview.md#materializedview) basado en tablas MergeTree soporte de particionamiento, también.

Una partición es una combinación lógica de registros en una tabla por un criterio especificado. Puede establecer una partición por un criterio arbitrario, como por mes, por día o por tipo de evento. Cada partición se almacena por separado para simplificar las manipulaciones de estos datos. Al acceder a los datos, ClickHouse utiliza el subconjunto más pequeño de particiones posible.

La partición se especifica en el `PARTITION BY expr` cláusula cuando [creando una tabla](mergetree.md#table_engine-mergetree-creating-a-table). La clave de partición puede ser cualquier expresión de las columnas de la tabla. Por ejemplo, para especificar la partición por mes, utilice la expresión `toYYYYMM(date_column)`:

``` sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

La clave de partición también puede ser una tupla de expresiones (similar a la [clave primaria](mergetree.md#primary-keys-and-indexes-in-queries)). Por ejemplo:

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

En este ejemplo, establecemos la partición por los tipos de eventos que se produjeron durante la semana actual.

Al insertar datos nuevos en una tabla, estos datos se almacenan como una parte separada (porción) ordenada por la clave principal. En 10-15 minutos después de insertar, las partes de la misma partición se fusionan en toda la parte.

!!! info "INFO"
    Una combinación solo funciona para partes de datos que tienen el mismo valor para la expresión de partición. Esto significa **no deberías hacer particiones demasiado granulares** (más de un millar de particiones). De lo contrario, el `SELECT` consulta funciona mal debido a un número excesivamente grande de archivos en el sistema de archivos y descriptores de archivos abiertos.

Utilice el [sistema.parte](../../../operations/system-tables.md#system_tables-parts) tabla para ver las partes y particiones de la tabla. Por ejemplo, supongamos que tenemos un `visits` tabla con partición por mes. Vamos a realizar el `SELECT` consulta para el `system.parts` tabla:

``` sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

El `partition` columna contiene los nombres de las particiones. Hay dos particiones en este ejemplo: `201901` y `201902`. Puede utilizar este valor de columna para especificar el nombre de partición en [ALTER … PARTITION](#alter_manipulations-with-partitions) consulta.

El `name` columna contiene los nombres de las partes de datos de partición. Puede utilizar esta columna para especificar el nombre de la pieza [ALTER ATTACH PART](#alter_attach-partition) consulta.

Vamos a desglosar el nombre de la primera parte: `201901_1_3_1`:

-   `201901` es el nombre de la partición.
-   `1` es el número mínimo del bloque de datos.
-   `3` es el número máximo del bloque de datos.
-   `1` es el nivel de fragmento (la profundidad del árbol de fusión del que se forma).

!!! info "INFO"
    Las partes de las tablas de tipo antiguo tienen el nombre: `20190117_20190123_2_2_0` (fecha mínima - fecha máxima - número de bloque mínimo - número de bloque máximo - nivel).

El `active` columna muestra el estado de la pieza. `1` está activo; `0` está inactivo. Las partes inactivas son, por ejemplo, las partes de origen que quedan después de fusionarse con una parte más grande. Las partes de datos dañadas también se indican como inactivas.

Como puede ver en el ejemplo, hay varias partes separadas de la misma partición (por ejemplo, `201901_1_3_1` y `201901_1_9_2`). Esto significa que estas partes aún no están fusionadas. ClickHouse combina las partes insertadas de datos periódicamente, aproximadamente 15 minutos después de la inserción. Además, puede realizar una fusión no programada utilizando el [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) consulta. Ejemplo:

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

``` text
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

Las partes inactivas se eliminarán aproximadamente 10 minutos después de la fusión.

Otra forma de ver un conjunto de partes y particiones es ir al directorio de la tabla: `/var/lib/clickhouse/data/<database>/<table>/`. Por ejemplo:

``` bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

Carpeta ‘201901_1_1_0’, ‘201901_1_7_1’ y así sucesivamente son los directorios de las partes. Cada parte se relaciona con una partición correspondiente y contiene datos solo para un mes determinado (la tabla de este ejemplo tiene particiones por mes).

El `detached` el directorio contiene partes que se separaron de la tabla utilizando el [DETACH](../../../sql-reference/statements/alter.md#alter_detach-partition) consulta. Las partes dañadas también se mueven a este directorio, en lugar de eliminarse. El servidor no utiliza las piezas del `detached` directory. You can add, delete, or modify the data in this directory at any time – the server will not know about this until you run the [ATTACH](../../../sql-reference/statements/alter.md#alter_attach-partition) consulta.

Tenga en cuenta que en el servidor operativo, no puede cambiar manualmente el conjunto de piezas o sus datos en el sistema de archivos, ya que el servidor no lo sabrá. Para tablas no replicadas, puede hacer esto cuando se detiene el servidor, pero no se recomienda. Para tablas replicadas, el conjunto de piezas no se puede cambiar en ningún caso.

ClickHouse le permite realizar operaciones con las particiones: eliminarlas, copiar de una tabla a otra o crear una copia de seguridad. Consulte la lista de todas las operaciones en la sección [Manipulaciones con particiones y piezas](../../../sql-reference/statements/alter.md#alter_manipulations-with-partitions).

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
