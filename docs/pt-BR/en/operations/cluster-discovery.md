---
description: 'Documentação sobre descoberta de cluster no ClickHouse'
sidebar_label: 'Descoberta de cluster'
slug: /operations/cluster-discovery
title: 'Descoberta de cluster'
doc_type: 'guide'
---

<div id="overview">
  ## Visão geral
</div>

O recurso Descoberta de cluster do ClickHouse simplifica a configuração de clusters, permitindo que os nós sejam descobertos e se registrem automaticamente, sem a necessidade de defini-los explicitamente nos arquivos de configuração. Isso é especialmente útil quando a definição manual de cada nó se torna trabalhosa.

:::note

A Descoberta de cluster é um recurso experimental e pode ser alterado ou removido em versões futuras.
Para habilitá-lo, inclua a configuração `allow_experimental_cluster_discovery` no seu arquivo de configuração:

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## Configuração de servidores remotos
</div>

<div id="traditional-manual-configuration">
  ### Configuração manual tradicional
</div>

Tradicionalmente, no ClickHouse, cada shard e réplica do cluster precisava ser especificado manualmente na configuração:

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
  ### Usando a Descoberta de cluster
</div>

Com a Descoberta de cluster, em vez de definir cada nó explicitamente, basta especificar um caminho no ZooKeeper. Todos os nós que se registrarem nesse caminho no ZooKeeper serão descobertos automaticamente e adicionados ao cluster.

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

Se quiser especificar um número de shard para um nó específico, você pode incluir a tag `<shard>` na seção `<discovery>`:

para `node1` e `node2`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

para `node3` e `node4`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### Modo observador
</div>

Os nós configurados no modo observador não se registrarão como réplicas.
Eles apenas observarão e descobrirão outras réplicas ativas no cluster, sem participar ativamente.
Para habilitar o modo observador, inclua a tag `<observer/>` na seção `<discovery>`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### Descoberta de clusters
</div>

Às vezes, pode ser necessário adicionar e remover não apenas hosts em clusters, mas também os próprios clusters. Você pode usar o nó `<multicluster_root_path>` com o caminho raiz de vários clusters:

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

Nesse caso, quando algum outro host se registrar no caminho `/clickhouse/discovery/some_new_cluster`, um cluster com nome `some_new_cluster` será adicionado.

Você pode usar os dois recursos simultaneamente; o host pode se registrar no cluster `my_cluster` e descobrir automaticamente quaisquer outros clusters:

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

Limitações:

* Você não pode usar `<path>` e `<multicluster_root_path>` ao mesmo tempo na mesma subárvore de `remote_servers`.
* `<multicluster_root_path>` só pode ser usado com `<observer/>`.
* A última parte do path do Keeper é usada como nome do cluster, enquanto, durante o registro, o nome é obtido da tag XML.

<div id="use-cases-and-limitations">
  ## Casos de uso e limitações
</div>

À medida que nós são adicionados ou removidos do caminho do ZooKeeper especificado, eles são automaticamente descobertos ou removidos do cluster sem a necessidade de alterar a configuração ou reiniciar o servidor.

No entanto, as alterações afetam apenas a configuração do cluster, não os dados nem os bancos de dados e as tabelas existentes.

Considere o exemplo a seguir com um cluster de 3 nós:

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

Em seguida, adicionamos um novo nó ao cluster, iniciando-o com a mesma entrada na seção `remote_servers` de um arquivo de configuração:

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

O quarto nó já faz parte do cluster, mas a tabela `event_table` ainda existe apenas nos três primeiros nós:

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

Se precisar que as tabelas sejam replicadas em todos os nós, você pode usar o motor de banco de dados [Replicated](../engines/database-engines/replicated.md) como alternativa à descoberta de cluster.