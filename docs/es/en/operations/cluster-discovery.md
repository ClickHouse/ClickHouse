---
description: 'Documentación de Cluster Discovery en ClickHouse'
sidebar_label: 'Cluster Discovery'
slug: /operations/cluster-discovery
title: 'Cluster Discovery'
doc_type: 'guide'
---

<div id="overview">
  ## Descripción general
</div>

La funcionalidad Cluster Discovery de ClickHouse simplifica la configuración del clúster al permitir que los nodos se descubran y se registren automáticamente, sin necesidad de definirlos explícitamente en los archivos de configuración. Esto resulta especialmente útil cuando la definición manual de cada nodo se vuelve engorrosa.

:::note

Cluster Discovery es una funcionalidad experimental y puede modificarse o eliminarse en versiones futuras.
Para habilitarla, incluya la opción `allow_experimental_cluster_discovery` en su archivo de configuración:

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## Configuración de servidores remotos
</div>

<div id="traditional-manual-configuration">
  ### Configuración manual tradicional
</div>

Tradicionalmente, en ClickHouse, era necesario especificar manualmente en la configuración cada segmento y réplica del clúster:

```xml
<remote_servers>
    <cluster_name>
        <shard>
            <replica>
                <host>node1</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node2</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>node3</host>
                <port>9000</port>
            </replica>
            <replica>
                <host>node4</host>
                <port>9000</port>
            </replica>
        </shard>
    </cluster_name>
</remote_servers>

```

<div id="using-cluster-discovery">
  ### Uso de Cluster Discovery
</div>

Con Cluster Discovery, en lugar de definir cada nodo explícitamente, basta con especificar una ruta en ZooKeeper. Todos los nodos que se registren en esa ruta de ZooKeeper se detectarán automáticamente y se añadirán al clúster.

```xml
<remote_servers>
    <cluster_name>
        <discovery>
            <path>/clickhouse/discovery/cluster_name</path>

            <!-- # Optional configuration parameters: -->

            <!-- ## Authentication credentials to access all other nodes in cluster: -->
            <!-- <user>user1</user> -->
            <!-- <password>pass123</password> -->
            <!-- ### Alternatively to password, interserver secret may be used: -->
            <!-- <secret>secret123</secret> -->

            <!-- ## Shard for current node (see below): -->
            <!-- <shard>1</shard> -->

            <!-- ## Observer mode (see below): -->
            <!-- <observer/> -->
        </discovery>
    </cluster_name>
</remote_servers>
```

Si desea especificar un número de segmento para un nodo concreto, puede incluir la etiqueta `<shard>` dentro de la sección `<discovery>`:

para `node1` y `node2`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

para `node3` y `node4`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### Modo observador
</div>

Los nodos configurados en modo observador no se registrarán como réplicas.
Se limitarán a observar y detectar otras réplicas activas en el clúster sin participar activamente.
Para habilitar el modo observador, incluya la etiqueta `<observer/>` dentro de la sección `<discovery>`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### Descubrimiento de clústeres
</div>

A veces puede ser necesario añadir y eliminar no solo hosts dentro de los clústeres, sino también los propios clústeres. Puede usar el nodo `<multicluster_root_path>` con la ruta raíz para varios clústeres:

```xml
<remote_servers>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

En este caso, cuando otro host se registre con la ruta `/clickhouse/discovery/some_new_cluster`, se añadirá un clúster con el nombre `some_new_cluster`.

Puede usar ambas funciones simultáneamente; el host puede registrarse en el clúster `my_cluster` y descubrir cualquier otro clúster:

```xml
<remote_servers>
    <my_cluster>
        <discovery>
            <path>/clickhouse/discovery/my_cluster</path>
        </discovery>
    </my_cluster>
    <some_unused_name>
        <discovery>
            <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            <observer/>
        </discovery>
    </some_unused_name>
</remote_servers>
```

Limitaciones:

* No puedes usar tanto `<path>` como `<multicluster_root_path>` en el mismo subárbol de `remote_servers`.
* `<multicluster_root_path>` solo puede usarse con `<observer/>`.
* La última parte de la ruta de Keeper se usa como nombre del clúster, mientras que durante el registro el nombre se toma de la etiqueta XML.

<div id="use-cases-and-limitations">
  ## Casos de uso y limitaciones
</div>

A medida que se añaden o eliminan nodos de la ruta de ZooKeeper especificada, estos se detectan o se eliminan automáticamente del clúster, sin necesidad de cambiar la configuración ni de reiniciar el servidor.

Sin embargo, los cambios afectan únicamente a la configuración del clúster, no a los datos ni a las bases de datos y tablas existentes.

Considere el siguiente ejemplo con un clúster de 3 nodos:

```xml
<remote_servers>
    <default>
        <discovery>
            <path>/clickhouse/discovery/default_cluster</path>
        </discovery>
    </default>
</remote_servers>
```

```sql
SELECT * EXCEPT (default_database, errors_count, slowdowns_count, estimated_recovery_time, database_shard_name, database_replica_name)
FROM system.clusters WHERE cluster = 'default';

┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

```sql
CREATE TABLE event_table ON CLUSTER default (event_time DateTime, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/event_table', '{replica}')
ORDER BY event_time PARTITION BY toYYYYMM(event_time);

INSERT INTO event_table ...
```

A continuación, añadimos un nuevo nodo al clúster, poniendo en marcha un nuevo nodo con la misma entrada en la sección `remote_servers` de un archivo de configuración:

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

El cuarto nodo forma parte del clúster, pero la tabla `event_table` todavía solo existe en los tres primeros nodos:

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

Si necesita que las tablas estén replicadas en todos los nodos, puede usar el motor de base de datos [Replicated](../engines/database-engines/replicated.md) como alternativa a Cluster Discovery.