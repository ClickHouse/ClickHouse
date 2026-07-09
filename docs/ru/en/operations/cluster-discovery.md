---
description: 'Документация по функции обнаружения кластера в ClickHouse'
sidebar_label: 'Обнаружение кластера'
slug: /operations/cluster-discovery
title: 'Обнаружение кластера'
doc_type: 'guide'
---

<div id="overview">
  ## Обзор
</div>

Функция обнаружения кластера в ClickHouse упрощает настройку кластера, позволяя узлам автоматически обнаруживать друг друга и регистрироваться без необходимости явно задавать их в конфигурационных файлах. Это особенно полезно, когда ручное определение каждого узла становится слишком трудоёмким.

:::note

Обнаружение кластера — экспериментальная возможность, которая может быть изменена или удалена в будущих версиях.
Чтобы включить её, добавьте настройку `allow_experimental_cluster_discovery` в конфигурационный файл:

```xml
<clickhouse>
    <!-- ... -->
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <!-- ... -->
</clickhouse>
```

:::

<div id="remote-servers-configuration">
  ## Конфигурация удаленных серверов
</div>

<div id="traditional-manual-configuration">
  ### Традиционная ручная конфигурация
</div>

Традиционно в ClickHouse каждый сегмент и каждую реплику в кластере нужно было указывать в конфигурации вручную:

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
  ### Использование обнаружения кластера
</div>

В обнаружении кластера вместо явного определения каждого узла достаточно просто указать path в ZooKeeper. Все узлы, зарегистрированные по этому path в ZooKeeper, будут автоматически обнаружены и добавлены в cluster.

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

Если вы хотите указать номер сегмента для определённого узла, добавьте тег `<shard>` в раздел `<discovery>`:

для `node1` и `node2`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>1</shard>
</discovery>
```

для `node3` и `node4`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <shard>2</shard>
</discovery>
```

<div id="observer-mode">
  ### Режим наблюдателя
</div>

Узлы, настроенные в режиме наблюдателя, не будут регистрироваться как реплики.
Они будут только отслеживать и обнаруживать другие активные реплики в кластере, сами не участвуя в работе.
Чтобы включить режим наблюдателя, добавьте тег `<observer/>` в раздел `<discovery>`:

```xml
<discovery>
    <path>/clickhouse/discovery/cluster_name</path>
    <observer/>
</discovery>
```

<div id="discovery-of-clusters">
  ### Обнаружение кластеров
</div>

Иногда может потребоваться добавлять и удалять не только хосты в кластерах, но и сами кластеры. Для этого можно использовать узел `<multicluster_root_path>` с корневым путём для нескольких кластеров:

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

В этом случае, когда какой-либо другой хост регистрируется по пути `/clickhouse/discovery/some_new_cluster`, будет добавлен кластер с именем `some_new_cluster`.

Вы можете использовать обе возможности одновременно: хост может зарегистрироваться в кластере `my_cluster` и обнаруживать другие кластеры:

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

Ограничения:

* Нельзя одновременно использовать `<path>` и `<multicluster_root_path>` в одном поддереве `remote_servers`.
* `<multicluster_root_path>` можно использовать только с `<observer/>`.
* Последняя часть path из Keeper используется как имя кластера, а при регистрации имя берётся из XML-тега.

<div id="use-cases-and-limitations">
  ## Сценарии использования и ограничения
</div>

При добавлении или удалении узлов по указанному пути ZooKeeper они автоматически обнаруживаются или исключаются из кластера без необходимости изменять конфигурацию или перезапускать серверы.

Однако изменения затрагивают только конфигурацию кластера, а не данные или существующие базы данных и таблицы.

Рассмотрим следующий пример с кластером из 3 узлов:

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

Затем добавим в кластер новый узел, запустив его с той же записью в разделе `remote_servers` файла конфигурации:

```response
┌─cluster─┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────┬─host_address─┬─port─┬─is_local─┬─user─┬─is_active─┐
│ default │         1 │            1 │           1 │ 92d3c04025e8 │ 172.26.0.5   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           2 │ a6a68731c21b │ 172.26.0.4   │ 9000 │        1 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           3 │ 8e62b9cb17a1 │ 172.26.0.2   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
│ default │         1 │            1 │           4 │ b0df3669b81f │ 172.26.0.6   │ 9000 │        0 │      │      ᴺᵁᴸᴸ │
└─────────┴───────────┴──────────────┴─────────────┴──────────────┴──────────────┴──────┴──────────┴──────┴───────────┘
```

Четвертый узел входит в кластер, но таблица `event_table` по-прежнему существует только на первых трех узлах:

```sql
SELECT hostname(), database, table FROM clusterAllReplicas(default, system.tables) WHERE table = 'event_table' FORMAT PrettyCompactMonoBlock

┌─hostname()───┬─database─┬─table───────┐
│ a6a68731c21b │ default  │ event_table │
│ 92d3c04025e8 │ default  │ event_table │
│ 8e62b9cb17a1 │ default  │ event_table │
└──────────────┴──────────┴─────────────┘
```

Если вам нужно, чтобы таблицы реплицировались на всех узлах, вы можете использовать движок базы данных [Replicated](../engines/database-engines/replicated.md) в качестве альтернативы обнаружению кластера.