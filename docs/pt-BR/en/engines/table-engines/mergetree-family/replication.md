---
description: 'Visão geral da replicação de dados com a família de motores de tabela Replicated* no ClickHouse'
sidebar_label: 'Replicated*'
sidebar_position: 20
slug: /engines/table-engines/mergetree-family/replication
title: 'Motores de tabela Replicated*'
doc_type: 'reference'
---

:::note
No ClickHouse Cloud, a replicação é gerenciada para você. Crie suas tabelas sem adicionar argumentos. Por exemplo, no texto abaixo, você substituiria:

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

com:

```sql
ENGINE = ReplicatedMergeTree
```

:::

A replicação é suportada apenas para tabelas da família MergeTree

* ReplicatedSummingMergeTree
* ReplicatedCoalescingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedGraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree

A replicação funciona no nível de uma tabela individual, não do servidor inteiro. Um servidor pode armazenar tabelas replicadas e não replicadas ao mesmo tempo.

A replicação não depende de sharding. Cada shard possui sua própria replicação independente.

Os dados comprimidos para as consultas `INSERT` e `ALTER` são replicados (para mais informações, consulte a documentação de [ALTER](/pt-BR/sql-reference/statements/alter).

As consultas `CREATE`, `DROP`, `ATTACH`, `DETACH` e `RENAME` são executadas em um único servidor e não são replicadas:

* A consulta `CREATE TABLE` cria uma nova tabela replicável no servidor onde a consulta é executada. Se essa tabela já existir em outros servidores, uma nova réplica é adicionada.
* A consulta `DROP TABLE` exclui a réplica localizada no servidor onde a consulta é executada.
* A consulta `RENAME` renomeia a tabela em uma das réplicas. Em outras palavras, tabelas replicadas podem ter nomes diferentes em réplicas distintas.

O ClickHouse utiliza o [ClickHouse Keeper](/pt-BR/guides/sre/keeper/index.md) para armazenar metadados de réplica. É possível usar o ZooKeeper versão 3.4.5 ou mais recente, mas o ClickHouse Keeper é recomendado.

Para usar a replicação, defina os parâmetros na seção de configuração do servidor [zookeeper](/pt-BR/operations/server-configuration-parameters/settings#zookeeper).

:::note
Não negligencie as configurações de segurança. O ClickHouse suporta o `digest` [ACL scheme](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) do subsistema de segurança do ZooKeeper.
:::

Exemplo de configuração dos endereços do cluster do ClickHouse Keeper:

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

O ClickHouse também oferece suporte ao armazenamento de metadado de réplica em um cluster ZooKeeper auxiliar. Faça isso fornecendo o nome e o caminho do cluster ZooKeeper como argumentos do mecanismo.
Em outras palavras, ele oferece suporte ao armazenamento dos metadados de tabelas diferentes em clusters ZooKeeper diferentes.

Exemplo de configuração dos endereços do cluster ZooKeeper auxiliar:

```xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

Para armazenar os metadados da tabela em um cluster ZooKeeper auxiliar em vez do cluster ZooKeeper padrão, podemos usar SQL para criar a tabela com o
motor ReplicatedMergeTree da seguinte forma:

```sql
CREATE TABLE table_name ( ... ) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```

Você pode especificar qualquer cluster existente do ZooKeeper, e o sistema usará um diretório nele para seus próprios dados (o diretório é especificado ao criar uma tabela replicável).

Se o ZooKeeper não estiver definido no arquivo de configuração, você não poderá criar tabelas replicadas, e quaisquer tabelas replicadas existentes ficarão somente leitura.

O ZooKeeper não é usado em consultas `SELECT`, porque a replicação não afeta o desempenho de `SELECT`, e as consultas são executadas tão rapidamente quanto em tabelas não replicadas. Ao consultar tabelas replicadas distribuídas, o comportamento do ClickHouse é controlado pelas configurações [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](/pt-BR/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) e [fallback&#95;to&#95;stale&#95;replicas&#95;for&#95;distributed&#95;queries](/pt-BR/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries).

Para cada consulta `INSERT`, aproximadamente dez entradas são adicionadas ao ZooKeeper por meio de várias transações. (Mais precisamente, isso ocorre para cada bloco de dados inserido; uma consulta `INSERT` contém um bloco, ou um bloco a cada `max_insert_block_size = 1048576` linhas.) Isso resulta em latências um pouco maiores para `INSERT` em comparação com tabelas não replicadas. Mas, se você seguir a recomendação de inserir dados em lotes, com no máximo um `INSERT` por segundo, isso não causará problemas. Todo o cluster ClickHouse que usa um cluster ZooKeeper para coordenação suporta, no total, várias centenas de `INSERTs` por segundo. A taxa de transferência das inserções de dados (o número de linhas por segundo) é tão alta quanto a de dados não replicados.

Para clusters muito grandes, você pode usar clusters ZooKeeper diferentes para shards diferentes. No entanto, pela nossa experiência, isso não se mostrou necessário em clusters de produção com aproximadamente 300 servidores.

A replicação é assíncrona e multi-master. Consultas `INSERT` (assim como `ALTER`) podem ser enviadas para qualquer servidor disponível. Os dados são inseridos no servidor onde a consulta é executada e depois copiados para os outros servidores. Como ela é assíncrona, os dados inseridos recentemente aparecem nas outras réplicas com alguma latência. Se parte das réplicas não estiver disponível, os dados serão gravados quando elas voltarem a ficar disponíveis. Se uma réplica estiver disponível, a latência será o tempo necessário para transferir o bloco de dados comprimidos pela rede. O número de threads que executam tarefas em segundo plano para tabelas replicadas pode ser definido pela configuração [background&#95;schedule&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size).

O motor `ReplicatedMergeTree` usa um pool de threads separado para replicated fetches. O tamanho do pool é limitado pela configuração [background&#95;fetches&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings#background_fetches_pool_size), que pode ser ajustada com a reinicialização do servidor.

Por padrão, uma consulta `INSERT` espera a confirmação da gravação dos dados de apenas uma réplica. Se os dados tiverem sido gravados com sucesso em apenas uma réplica e o servidor com essa réplica deixar de existir, os dados armazenados serão perdidos. Para habilitar a confirmação da gravação dos dados em várias réplicas, use a opção `insert_quorum`.

Cada bloco de dados é gravado atomicamente. A consulta `INSERT` é dividida em blocos de até `max_insert_block_size = 1048576` linhas. Em outras palavras, se a consulta `INSERT` tiver menos de 1048576 linhas, ela será feita atomicamente.

Os blocos de dados são desduplicados. Em caso de várias gravações do mesmo bloco de dados (blocos de dados do mesmo tamanho, contendo as mesmas linhas na mesma ordem), o bloco é gravado apenas uma vez. O motivo disso é que, em caso de falhas de rede, a aplicação cliente pode não saber se os dados foram gravados no banco de dados, então a consulta `INSERT` pode simplesmente ser repetida. Não importa para qual réplica os `INSERTs` com dados idênticos foram enviados. `INSERTs` são idempotentes. Os parâmetros de desduplicação são controlados pelas configurações do servidor [merge&#95;tree](/pt-BR/operations/server-configuration-parameters/settings.md/#merge_tree).

Durante a replicação, apenas os dados de origem a serem inseridos são transferidos pela rede. As demais transformações de dados (merging) são coordenadas e executadas da mesma forma em todas as réplicas. Isso minimiza o uso da rede, o que significa que a replicação funciona bem quando as réplicas estão em datacenters diferentes. (Observe que duplicar dados em datacenters diferentes é o principal objetivo da replicação.)

Você pode ter qualquer número de réplicas dos mesmos dados. Com base em nossa experiência, uma solução relativamente confiável e prática pode usar replicação dupla em produção, com cada servidor usando RAID-5 ou RAID-6 (e RAID-10 em alguns casos).

O sistema monitora a sincronização dos dados nas réplicas e consegue se recuperar após uma falha. O failover é automático (para pequenas diferenças nos dados) ou semiautomático (quando os dados diferem demais, o que pode indicar um erro de configuração).

<div id="creating-replicated-tables">
  ## Criando tabelas replicadas
</div>

:::note
No ClickHouse Cloud, a replicação é gerenciada automaticamente.

Crie tabelas usando [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree) sem argumentos de replicação. Internamente, o sistema reescreve [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree) como [`SharedMergeTree`](/pt-BR/cloud/reference/shared-merge-tree) para fins de replicação e distribuição de dados.

Evite usar `ReplicatedMergeTree` ou especificar parâmetros de replicação, pois ela é gerenciada pela plataforma.

:::

<div id="replicatedmergetree-parameters">
  ### Parâmetros de Replicated*MergeTree
</div>

| Parâmetro          | Descrição                                                                                                   |
| ------------------ | ----------------------------------------------------------------------------------------------------------- |
| `zoo_path`         | O caminho da tabela no ClickHouse Keeper.                                                                   |
| `replica_name`     | O nome da réplica no ClickHouse Keeper.                                                                     |
| `other_parameters` | Os parâmetros do motor usado para criar a versão replicada, por exemplo, a versão em `ReplacingMergeTree`. |

Exemplo:

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">
  <summary>Exemplo em sintaxe obsoleta</summary>

  ```sql
  CREATE TABLE table_name
  (
      EventDate DateTime,
      CounterID UInt32,
      UserID UInt32
  ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
  ```
</details>

Como mostrado no exemplo, esses parâmetros podem conter substituições em `{}`. Os valores substituídos são obtidos na seção [macros](/pt-BR/operations/server-configuration-parameters/settings.md/#macros) do arquivo de configuração.

Exemplo:

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

O caminho para a tabela no ClickHouse Keeper deve ser exclusivo para cada tabela replicada. Tabelas em shards diferentes devem ter caminhos diferentes.
Neste caso, o caminho consiste nas seguintes partes:

`/clickhouse/tables/` é o prefixo comum. Recomendamos usar exatamente este.

`{shard}` será expandido para o identificador do shard.

`table_name` é o nome do nó da tabela no ClickHouse Keeper. É uma boa ideia usar o mesmo nome da tabela. Ele é definido explicitamente porque, ao contrário do nome da tabela, não muda após uma consulta RENAME.
*DICA*: você também pode adicionar o nome do banco de dados antes de `table_name`. Por exemplo, `db_name.table_name`

As duas substituições internas `{database}` e `{table}` podem ser usadas; elas serão expandidas para o nome da tabela e o nome do banco de dados, respectivamente (a menos que essas macros estejam definidas na seção `macros`). Assim, o caminho no ZooKeeper pode ser especificado como `'/clickhouse/tables/{shard}/{database}/{table}'`.
Tenha cuidado ao renomear tabelas ao usar essas substituições internas. O caminho no ClickHouse Keeper não pode ser alterado e, quando a tabela é renomeada, as macros serão expandidas para um caminho diferente, a tabela passará a apontar para um caminho que não existe no ClickHouse Keeper e entrará em modo somente leitura.

O nome da réplica identifica réplicas diferentes da mesma tabela. Você pode usar o nome do servidor para isso, como no exemplo. O nome só precisa ser exclusivo dentro de cada shard.

Você pode definir os parâmetros explicitamente em vez de usar substituições. Isso pode ser conveniente para testes e para configurar clusters pequenos. No entanto, nesse caso, você não pode usar consultas DDL distribuídas (`ON CLUSTER`).

Ao trabalhar com clusters grandes, recomendamos usar substituições porque elas reduzem a probabilidade de erro.

Você pode especificar argumentos padrão para o motor de tabela `Replicated` no arquivo de configuração do servidor. Por exemplo:

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

Nesse caso, você pode omitir os argumentos ao criar tabelas:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

É equivalente a:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

Execute a consulta `CREATE TABLE` em cada réplica. Essa consulta cria uma nova tabela replicada ou adiciona uma nova réplica a uma já existente.

Se você adicionar uma nova réplica depois que a tabela já contiver dados em outras réplicas, os dados serão copiados das outras réplicas para a nova após a execução da consulta. Em outras palavras, a nova réplica se sincroniza com as demais.

Para excluir uma réplica, execute `DROP TABLE`. No entanto, apenas uma réplica é excluída — aquela que está no servidor em que você executa a consulta.

<div id="recovery-after-failures">
  ## Recuperação após falhas
</div>

Se o ClickHouse Keeper estiver indisponível quando um servidor iniciar, as tabelas replicadas passam para o modo somente leitura. O sistema tenta periodicamente se conectar ao ClickHouse Keeper.

Se o ClickHouse Keeper estiver indisponível durante um `INSERT` ou ocorrer um erro ao interagir com o ClickHouse Keeper, uma exceção é lançada.

Após se conectar ao ClickHouse Keeper, o sistema verifica se o conjunto de dados no sistema de arquivos local corresponde ao conjunto de dados esperado (o ClickHouse Keeper armazena essas informações). Se houver pequenas inconsistências, o sistema as corrige sincronizando os dados com as réplicas.

Se o sistema detectar partes de dados corrompidas (com tamanho de arquivo incorreto) ou partes não reconhecidas (partes gravadas no sistema de arquivos, mas não registradas no ClickHouse Keeper), ele as move para o subdiretório `detached` (elas não são excluídas). Quaisquer partes ausentes são copiadas das réplicas.

Observe que o ClickHouse não executa nenhuma ação destrutiva, como excluir automaticamente uma grande quantidade de dados.

Quando o servidor inicia (ou estabelece uma nova sessão com o ClickHouse Keeper), ele verifica apenas a quantidade e os tamanhos de todos os arquivos. Se os tamanhos dos arquivos corresponderem, mas alguns bytes tiverem sido alterados em algum ponto no meio, isso não será detectado imediatamente, mas apenas ao tentar ler os dados para uma consulta `SELECT`. A consulta lança uma exceção informando checksum incompatível ou tamanho incorreto de um bloco comprimido. Nesse caso, as partes de dados são adicionadas à fila de verificação e copiadas das réplicas, se necessário.

Se o conjunto local de dados diferir demais do esperado, um mecanismo de segurança é acionado. O servidor registra isso no log e se recusa a iniciar. Isso ocorre porque esse caso pode indicar um erro de configuração, como quando uma réplica em um shard foi configurada acidentalmente como uma réplica em outro shard. No entanto, os limiares desse mecanismo são definidos em um nível bastante baixo, e essa situação pode ocorrer durante uma recuperação normal de falhas. Nesse caso, os dados são restaurados de forma semiautomática — &quot;apertando um botão&quot;.

Para iniciar a recuperação, crie o nó `/path_to_table/replica_name/flags/force_restore_data` no ClickHouse Keeper com qualquer conteúdo ou execute o comando para restaurar todas as tabelas replicadas:

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Em seguida, reinicie o servidor. Ao iniciar, o servidor remove essas flags e inicia a recuperação.

<div id="recovery-after-complete-data-loss">
  ## Recuperação após perda completa de dados
</div>

Se todos os dados e metadados tiverem desaparecido de um dos servidores, siga estas etapas para a recuperação:

1. Instale o ClickHouse no servidor. Defina corretamente as substituições no arquivo de configuração que contém o identificador do shard e as réplicas, se você as usar.
2. Se você tinha tabelas não replicadas que precisam ser duplicadas manualmente nos servidores, copie os dados delas de uma réplica (no diretório `/var/lib/clickhouse/data/db_name/table_name/`).
3. Copie de uma réplica as definições das tabelas localizadas em `/var/lib/clickhouse/metadata/`. Se um identificador de shard ou de réplica estiver definido explicitamente nas definições das tabelas, corrija-o para que corresponda a esta réplica. (Como alternativa, inicie o servidor e execute todas as consultas `ATTACH TABLE` que deveriam estar nos arquivos .sql em `/var/lib/clickhouse/metadata/`.)
4. Para iniciar a recuperação, crie o nó do ClickHouse Keeper `/path_to_table/replica_name/flags/force_restore_data` com qualquer conteúdo, ou execute o comando para restaurar todas as tabelas replicadas: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Em seguida, inicie o servidor (ou reinicie-o, se já estiver em execução). Os dados serão baixados das réplicas.

Uma opção alternativa de recuperação é excluir as informações sobre a réplica perdida do ClickHouse Keeper (`/path_to_table/replica_name`) e, em seguida, criar a réplica novamente, conforme descrito em &quot;[Criando tabelas replicadas](#creating-replicated-tables)&quot;.

Não há restrição de largura de banda de rede durante a recuperação. Tenha isso em mente se estiver restaurando muitas réplicas de uma só vez.

<div id="converting-from-mergetree-to-replicatedmergetree">
  ## Convertendo de MergeTree para ReplicatedMergeTree
</div>

Usamos o termo `MergeTree` para nos referir a todos os motores de tabela da `família MergeTree`, assim como a `ReplicatedMergeTree`.

Se você tiver uma tabela `MergeTree` replicada manualmente, poderá convertê-la em uma tabela replicada. Isso pode ser necessário se você já tiver coletado uma grande quantidade de dados em uma tabela `MergeTree` e agora quiser habilitar a replicação.

A instrução [ATTACH TABLE ... AS REPLICATED](/pt-BR/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) permite anexar uma tabela `MergeTree` desanexada como `ReplicatedMergeTree`.

A tabela `MergeTree` pode ser convertida automaticamente na reinicialização do servidor se a flag `convert_to_replicated` estiver definida no diretório de dados da tabela (`/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` para o banco de dados `Atomic`).
Crie um arquivo vazio `convert_to_replicated`, e a tabela será carregada como replicada na próxima reinicialização do servidor.

Esta consulta pode ser usada para obter o caminho dos dados da tabela. Se a tabela tiver vários caminhos de dados, você deverá usar o primeiro.

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

Observe que a tabela ReplicatedMergeTree será criada com os valores das configurações `default_replica_path` e `default_replica_name`.
Para criar uma tabela convertida em outras réplicas, você precisará especificar explicitamente o caminho dela no primeiro argumento do motor `ReplicatedMergeTree`. A consulta a seguir pode ser usada para obter esse caminho.

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

Também existe uma forma manual de fazer isso.

Se os dados diferirem entre as várias réplicas, primeiro sincronize-os ou exclua esses dados de todas as réplicas, exceto de uma.

Renomeie a tabela MergeTree existente e, em seguida, crie uma tabela `ReplicatedMergeTree` com o nome antigo.
Mova os dados da tabela antiga para o subdiretório `detached` dentro do diretório com os dados da nova tabela (`/var/lib/clickhouse/data/db_name/table_name/`).
Em seguida, execute `ALTER TABLE ATTACH PARTITION` em uma das réplicas para adicionar essas partes de dados ao conjunto ativo.

<div id="converting-from-replicatedmergetree-to-mergetree">
  ## Convertendo de ReplicatedMergeTree para MergeTree
</div>

Use a instrução [ATTACH TABLE ... AS NOT REPLICATED](/pt-BR/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) para anexar uma tabela `ReplicatedMergeTree` desanexada como `MergeTree` em um único servidor.

Outra forma de fazer isso envolve reiniciar o servidor. Crie uma tabela MergeTree com um nome diferente. Mova todos os dados do diretório com os dados da tabela `ReplicatedMergeTree` para o diretório de dados da nova tabela. Em seguida, exclua a tabela `ReplicatedMergeTree` e reinicie o servidor.

Se você quiser se livrar de uma tabela `ReplicatedMergeTree` sem iniciar o servidor:

* Exclua o arquivo `.sql` correspondente no diretório de metadados (`/var/lib/clickhouse/metadata/`).
* Exclua o caminho correspondente no ClickHouse Keeper (`/path_to_table/replica_name`).

Depois disso, você pode iniciar o servidor, criar uma tabela `MergeTree`, mover os dados para o diretório dela e então reiniciar o servidor.

<div id="recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged">
  ## Recuperação quando os metadados no cluster do ClickHouse Keeper são perdidos ou corrompidos
</div>

Se os dados no ClickHouse Keeper tiverem sido perdidos ou corrompidos, você poderá salvá-los movendo-os para uma tabela não replicada, conforme descrito acima.

**Veja também**

* [background&#95;schedule&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
* [background&#95;fetches&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
* [execute&#95;merges&#95;on&#95;single&#95;replica&#95;time&#95;threshold](/pt-BR/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
* [max&#95;replicated&#95;fetches&#95;network&#95;bandwidth](/pt-BR/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
* [max&#95;replicated&#95;sends&#95;network&#95;bandwidth](/pt-BR/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)