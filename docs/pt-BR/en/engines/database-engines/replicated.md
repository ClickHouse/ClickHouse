---
description: 'O motor é baseado no motor Atomic. Ele oferece suporte à replicação de
  metadados por meio do log de DDL gravado no ZooKeeper e executado em todas as réplicas
  de um determinado banco de dados.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'reference'
---

O motor é baseado no motor [Atomic](../../engines/database-engines/atomic.md). Ele oferece suporte à replicação de metadados por meio do log de DDL gravado no ZooKeeper e executado em todas as réplicas de um determinado banco de dados.

Um servidor ClickHouse pode ter vários bancos de dados replicados em execução e sendo atualizados ao mesmo tempo. No entanto, não pode haver várias réplicas do mesmo banco de dados replicado.

<div id="creating-a-database">
  ## Criar um banco de dados
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Parâmetros do motor**

* `zoo_path` — Caminho do ZooKeeper. O mesmo caminho do ZooKeeper corresponde ao mesmo banco de dados.
* `shard_name` — Nome do shard. As réplicas do banco de dados são agrupadas em shards por `shard_name`.
* `replica_name` — Nome da réplica. Os nomes das réplicas devem ser diferentes entre todas as réplicas do mesmo shard.

Os parâmetros podem ser omitidos; nesse caso, os parâmetros ausentes são substituídos pelos valores padrão.

Se `zoo_path` contiver a macro `{uuid}`, será necessário especificar um UUID explícito ou adicionar [ON CLUSTER](../../sql-reference/distributed-ddl.md) à instrução CREATE para garantir que todas as réplicas usem o mesmo UUID para esse banco de dados.

Para tabelas [ReplicatedMergeTree](/pt-BR/engines/table-engines/mergetree-family/replication), se nenhum argumento for fornecido, serão usados os argumentos padrão: `/clickhouse/tables/{uuid}/{shard}` e `{replica}`. Eles podem ser alterados nas configurações do servidor [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) e [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name). A macro `{uuid}` é expandida para o uuid da tabela; `{shard}` e `{replica}` são expandidos para valores da configuração do servidor, e não dos argumentos do motor do banco de dados. No futuro, porém, será possível usar `shard_name` e `replica_name` do banco de dados Replicated.

Também há suporte a cluster ZooKeeper auxiliar para armazenar os metadados de um banco de dados replicado, em vez de usar o cluster ZooKeeper padrão. Podemos usar SQL para criar o banco de dados replicado com um cluster ZooKeeper auxiliar da seguinte forma:

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## Especificidades e recomendações
</div>

As consultas DDL com banco de dados `Replicated` funcionam de forma semelhante às consultas [ON CLUSTER](../../sql-reference/distributed-ddl.md), mas com pequenas diferenças.

Primeiro, a solicitação DDL tenta ser executada no iniciador (o host que recebeu originalmente a solicitação do usuário). Se a solicitação não for concluída, o usuário receberá um erro imediatamente, e os outros hosts não tentarão executá-la. Se a solicitação for concluída com sucesso no iniciador, todos os outros hosts tentarão novamente automaticamente até concluí-la. O iniciador tentará aguardar a conclusão da consulta nos outros hosts (por no máximo [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) e retornará uma tabela com os status de execução da consulta em cada host.

O comportamento em caso de erros é controlado pela configuração [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode); para um banco de dados `Replicated`, é melhor defini-la como `null_status_on_timeout` — ou seja, se alguns hosts não tiverem tempo de executar a solicitação dentro de [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), não gere uma exceção, mas mostre o status `NULL` deles na tabela.

A tabela de sistema [system.clusters](../../operations/system-tables/clusters.md) contém um cluster com nome igual ao do banco de dados replicado, que consiste em todas as réplicas do banco de dados. Esse cluster é atualizado automaticamente quando réplicas são criadas/excluídas e pode ser usado para tabelas [Distributed](/pt-BR/engines/table-engines/special/distributed).

Ao criar uma nova réplica do banco de dados, essa réplica cria as tabelas por conta própria. Se a réplica tiver ficado indisponível por muito tempo e estiver defasada em relação ao log de replicação, ela verificará os metadados locais em relação aos metadados atuais no ZooKeeper, moverá as tabelas extras com dados para um banco de dados separado não replicado (para não excluir acidentalmente nada desnecessário), criará as tabelas ausentes e atualizará os nomes das tabelas caso tenham sido renomeadas. Os dados são replicados no nível de `ReplicatedMergeTree`, ou seja, se a tabela não for replicada, os dados não serão replicados (o banco de dados é responsável apenas pelos metadados).

As consultas [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md) são permitidas, mas não são replicadas. O motor de banco de dados apenas adicionará/buscará/removerá a partição/parte na réplica atual. No entanto, se a própria tabela usar um motor de tabela Replicated, os dados serão replicados após o uso de `ATTACH`.

Caso você precise apenas configurar um cluster sem manter a replicação de tabelas, consulte o recurso [Cluster Discovery](../../operations/cluster-discovery.md).

<div id="usage-example">
  ## Exemplo de uso
</div>

Criando um cluster com três hosts:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Criando um banco de dados no cluster com parâmetros implícitos:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

Executando a instrução DDL:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Exibindo a tabela do sistema:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Criando uma tabela distribuída e inserindo os dados:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Adicionando uma réplica em mais um host:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Adicionando uma réplica em outro host se a macro `{uuid}` for usada em `zoo_path`:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

A configuração do cluster ficará assim:

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

A tabela distribuída também passará a obter dados do novo host:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## Configurações
</div>

As seguintes configurações são suportadas:

| Setting                                                                      | Default                        | Description                                                                                                                                                                                                                                                                                                                                               |
| ---------------------------------------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | Não recupere a réplica automaticamente se a razão entre tabelas desatualizadas e o total de tabelas for maior                                                                                                                                                                                                                                             |
| `max_replication_lag_to_enqueue`                                             | 50                             | A réplica gerará uma exceção ao tentar executar uma consulta se sua defasagem de replicação for maior                                                                                                                                                                                                                                                     |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | As réplicas tentarão cancelar a consulta se o tempo limite for excedido, mas o host iniciador ainda não a tiver executado                                                                                                                                                                                                                                 |
| `collection_name`                                                            |                                | Nome de uma coleção definida na configuração do servidor, onde todas as informações de autenticação do cluster são definidas                                                                                                                                                                                                                              |
| `check_consistency`                                                          | true                           | Verifica a consistência dos metadados locais e dos metadados no Keeper, realizando a recuperação da réplica em caso de inconsistência                                                                                                                                                                                                                     |
| `max_retries_before_automatic_recovery`                                      | 10                             | Número máximo de tentativas de executar uma entrada da fila antes de marcar a réplica como perdida e recuperá-la a partir de um snapshot (0 significa infinito)                                                                                                                                                                                           |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | Se habilitado, ao processar DDLs em bancos de dados Replicated, ignora a criação e a troca de DDLs das tabelas temporárias de visões materializadas atualizáveis, quando possível                                                                                                                                                                         |
| `logs_to_keep`                                                               | 1000                           | Número padrão de logs a manter no ZooKeeper para o banco de dados Replicated.                                                                                                                                                                                                                                                                             |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | O caminho para o banco de dados no ZooKeeper. Usado durante a criação do banco de dados se os argumentos forem omitidos.                                                                                                                                                                                                                                  |
| `default_replica_shard_name`                                                 | `{shard}`                      | O nome do shard da réplica no banco de dados. Usado durante a criação do banco de dados se os argumentos forem omitidos.                                                                                                                                                                                                                                  |
| `default_replica_name`                                                       | `{replica}`                    | O nome da réplica no banco de dados. Usado durante a criação do banco de dados se os argumentos forem omitidos.                                                                                                                                                                                                                                           |
| `internal_replication`                                                       | false                          | Se uma tabela Distributed criada com o cluster deste banco de dados Replicated enviará dados para uma das réplicas (replicação interna significa que as réplicas do cluster fazem a replicação por conta própria) ou para todas as réplicas (sem replicação interna significa que a tabela Distributed enviará os dados inseridos para todas as réplicas) |

Os valores padrão podem ser substituídos no arquivo de configuração

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```