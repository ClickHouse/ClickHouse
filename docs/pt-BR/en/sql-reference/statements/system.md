---
description: 'Documentação das Instruções SYSTEM'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'Instruções SYSTEM'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # Instruções SYSTEM
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

Recarrega todos os [dicionários internos](./create/dictionary/overview.md).
Por padrão, os dicionários internos ficam desativados.
Sempre retorna `Ok.` independentemente do resultado da atualização dos dicionários internos.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

A consulta `SYSTEM RELOAD DICTIONARIES` recarrega dicionários com status `LOADED` (consulte a coluna `status` de [`system.dictionaries`](/pt-BR/operations/system-tables/dictionaries)), ou seja, dicionários que já foram carregados com sucesso.
Por padrão, os dicionários são carregados de forma lazy (consulte [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)); assim, em vez de serem carregados automaticamente na inicialização, eles são inicializados no primeiro acesso, por meio da função [`dictGet`](/pt-BR/sql-reference/functions/ext-dict-functions#dictGet) ou do uso de `SELECT` em tabelas com `ENGINE = Dictionary`.

**Sintaxe**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

Recarrega completamente um dicionário `dictionary_name`, independentemente do estado do dicionário (LOADED / NOT&#95;LOADED / FAILED).
Retorna sempre `Ok.`, independentemente do resultado da atualização do dicionário.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

O status do dicionário pode ser verificado com uma consulta à tabela `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
Esta instrução e `SYSTEM RELOAD MODEL` apenas removem o carregamento de modelos CatBoost da clickhouse-library-bridge. A função `catboostEvaluate()`
carrega um modelo no primeiro acesso, caso ele ainda não esteja carregado.
:::

Remove o carregamento de todos os modelos CatBoost.

**Sintaxe**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

Descarrega um modelo CatBoost de `model_path`.

**Sintaxe**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

Recarrega todas as [funções executáveis definidas pelo usuário](/pt-BR/sql-reference/functions/udf#executable-user-defined-functions) registradas, ou uma delas, a partir de um arquivo de configuração.

**Sintaxe**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

Recalcula todas as [métricas assíncronas](../../operations/system-tables/asynchronous_metrics.md). Como as métricas assíncronas são atualizadas periodicamente com base na configuração [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md), em geral não é necessário atualizá-las manualmente com esta instrução.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

Limpa o cache DNS interno do ClickHouse. Às vezes (em versões antigas do ClickHouse), é necessário usar esse comando ao alterar a infraestrutura (mudando o endereço IP de outro servidor ClickHouse ou do servidor usado pelos dicionários).

Para um gerenciamento de cache mais prático (automático), consulte os parâmetros `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period`.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

Limpa o cache de marca.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

Limpa o cache do índice primário, que mantém as chaves primárias das tabelas [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) em memória.
Seu tamanho é configurado pela definição no nível do servidor [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size).

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Limpa o cache de metadados do Iceberg.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

Limpa os caches por URL do Confluent Schema Registry usados pelo formato `AvroConfluent`. Isso remove tanto o cache de busca de esquemas (id → esquema) quanto o cache de registro de esquemas (subject + esquema → id), de modo que leituras e gravações subsequentes voltem a recorrer ao servidor de registro. Útil quando um esquema foi excluído ou reescrito no registro, ou para verificar a idempotência do registro em testes.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Limpa o cache de metadados do Parquet.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

Limpa o cache de polígonos constantes pré-processados usados pela função [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon). O limite de tamanho configurado (a configuração de servidor `point_in_polygon_cache_size`) permanece inalterado, portanto o cache continua aceitando entradas depois disso. Para desativar o cache, defina `point_in_polygon_cache_size` como `0`.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

Limpa os caches de tokens, cabeçalho e postings do índice de texto.

Se quiser limpar individualmente um desses caches, você pode executar

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE` ou
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

Limpa o cache de marcas dos índices secundários (data-skipping).

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

Limpa o cache de blocos não comprimidos dos índices secundários (data-skipping).

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

Limpa o cache de arquivos mapeados na memória.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

Limpa o cache de páginas em espaço de usuário, que é o cache em memória do próprio ClickHouse para dados lidos do armazenamento subjacente.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

Limpa o cache do índice de similaridade vetorial.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

Limpa o cache dos pools de conexões HTTP usados nas conexões de saída.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

Limpa o cache dos clientes S3.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

Carrega as marcas de uma tabela para o [cache de marcas](#drop-mark-cache). As marcas de índices secundários também são carregadas para o [cache de marcas do índice](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

Carrega os índices primários de uma tabela `MergeTree` para o [cache de índice primário](#drop-primary-index-cache).

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

Limpa o cache de metadados do disco especificado.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

Reconcilia o estado em memória do cache do sistema de arquivos do ClickHouse com os arquivos de cache efetivamente presentes no disco e retorna o `cache_name`, o `path` e o `size` baixado de cada segmento de arquivo em cache. Um nome de cache opcional limita a operação a um único cache.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` está disponível apenas no ClickHouse Cloud.
:::

Remove o cache distribuído. Use `CONNECTIONS` para remover apenas as conexões em cache com os servidores do cache distribuído ou informe um identificador de servidor para especificar um único servidor.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

Réplicas inativas de tabelas `ReplicatedMergeTree` podem ser removidas com a seguinte sintaxe:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

As consultas removerão o caminho da réplica `ReplicatedMergeTree` no ZooKeeper. Isso é útil quando a réplica está inativa e seus metadados não podem mais ser removidos do ZooKeeper com `DROP TABLE`, porque essa tabela já não existe. Isso removerá apenas a réplica inativa/obsoleta e não poderá remover a réplica local; para isso, use `DROP TABLE`. `DROP REPLICA` não remove nenhuma tabela nem remove dados ou metadados do disco.

A primeira remove os metadados da réplica `'replica_name'` da tabela `database.table`.
A segunda faz o mesmo para todas as tabelas replicadas no banco de dados.
A terceira faz o mesmo para todas as tabelas replicadas no servidor local.
A quarta é útil para remover os metadados de uma réplica inativa quando todas as outras réplicas de uma tabela foram removidas. Ela exige que o caminho da tabela seja especificado explicitamente. Deve ser o mesmo caminho que foi passado como primeiro argumento do engine `ReplicatedMergeTree` na criação da tabela.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

Réplicas inativas de bancos de dados `Replicated` podem ser removidas com a seguinte sintaxe:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Semelhante a `SYSTEM DROP REPLICA`, mas remove o caminho da réplica do banco de dados `Replicated` no ZooKeeper quando não há banco de dados para executar `DROP DATABASE`. Observe que isso não remove as réplicas `ReplicatedMergeTree` (portanto, talvez você também precise de `SYSTEM DROP REPLICA`). Os nomes do shard e da réplica são os nomes especificados nos argumentos do engine `Replicated` ao criar o banco de dados. Além disso, esses nomes podem ser obtidos nas colunas `database_shard_name` e `database_replica_name` de `system.clusters`. Se a cláusula `FROM SHARD` estiver ausente, `replica_name` deverá ser o nome completo da réplica no formato `shard_name|replica_name`.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP CACHE NÃO COMPRIMIDO
</div>

Limpa o cache de dados não comprimidos.
O cache de dados não comprimidos é habilitado/desabilitado pela configuração [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache) nos níveis de consulta/usuário/perfil.
Seu tamanho pode ser configurado por meio da configuração [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size) no nível do servidor.

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

Limpa o cache de expressões compiladas.
O cache de expressões compiladas é ativado/desativado pela configuração no nível de consulta/usuário/perfil [`compile_expressions`](../../operations/settings/settings.md#compile_expressions).

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

Limpa o cache de condições de consulta.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

Limpa o [cache de consultas](../../operations/query-cache.md).
Se uma tag for especificada, apenas as entradas do cache de consultas com essa tag serão excluídas.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

Limpa o cache dos esquemas carregados de [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

Alvos compatíveis:

* Protobuf: Remove da memória as definições importadas de mensagens Protobuf.
* Files: Exclui os arquivos de esquema em cache armazenados localmente em [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path), gerados quando `format_schema_source` é definido como `query`.
  Observação: se nenhum alvo for especificado, ambos os caches serão limpos.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

Descarrega mensagens de log em buffer para as tabelas de sistema, por exemplo `system.query&#95;log`. É útil principalmente para depuração, já que a maioria das tabelas de sistema tem um intervalo de descarregamento padrão de 7,5 segundos.
Isso também criará tabelas de sistema mesmo que a fila de mensagens esteja vazia.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

Se você não quiser fazer flush de tudo, pode fazer flush de um ou mais logs individuais informando o nome deles ou a tabela de destino:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

Recarrega a configuração do ClickHouse. É usado quando a configuração está armazenada no ZooKeeper. Observe que `SYSTEM RELOAD CONFIG` não recarrega a configuração de `USER` armazenada no ZooKeeper; ele recarrega apenas a configuração de `USER` armazenada em `users.xml`. Para recarregar toda a configuração de `USER`, use `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

Recarrega todos os armazenamentos de acesso, incluindo: users.xml, armazenamento de acesso em disco local e armazenamento de acesso replicado (no ZooKeeper).

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

Encerra o ClickHouse normalmente (como `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

Interrompe o processo do ClickHouse (como `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

Gerencia pontos de instrumentação usando o recurso XRay do LLVM, disponível quando o ClickHouse é compilado com `ENABLE_XRAY=1`.
Isso permite depurar e coletar perfis em produção sem modificar o código-fonte e com sobrecarga mínima.
Quando nenhum ponto de instrumentação é adicionado, a penalidade de desempenho é desprezível, porque isso só acrescenta um salto extra para um endereço próximo
no prólogo e no epílogo das funções com mais de 200 instruções.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

Adiciona um novo ponto de instrumentação. As funções instrumentadas podem ser inspecionadas na tabela de sistema [`system.instrumentation`](../../operations/system-tables/instrumentation.md). Mais de um handler pode ser adicionado à mesma função, e eles serão executados na mesma ordem em que a instrumentação for adicionada.
As funções a serem instrumentadas podem ser obtidas na tabela de sistema [`system.symbols`](../../operations/system-tables/symbols.md).

Há três tipos diferentes de handlers que podem ser adicionados às funções:

**Sintaxe**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

em que `FUNCTION` é qualquer função ou substring dela, como `QueryMetricLog::startQuery`, e o handler é um dos seguintes

<div id="instrument-add-log">
  #### LOG
</div>

Imprime o texto fornecido como argumento e o stack trace na `ENTRY` ou `EXIT` da função.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

Faz uma pausa por um número fixo de segundos em `ENTRY` ou `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

ou por uma quantidade aleatória de segundos com distribuição uniforme, fornecendo mínimo e máximo separados por um espaço em branco:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

Mede o tempo gasto entre `ENTRY` e `EXIT` de uma função.
O resultado do profiling é armazenado em [`system.trace_log`](../../operations/system-tables/trace_log.md) e pode ser convertido
para o [Chrome Event Trace Format](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

Remove um único ponto de instrumentação com:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

todos usando a palavra-chave `ALL`:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

um conjunto de IDs de uma subconsulta:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

ou todos os pontos de instrumentação que correspondem a um determinado function&#95;name:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

As informações do ponto de instrumentação podem ser coletadas a partir da tabela de sistema [`system.instrumentation`](../../operations/system-tables/instrumentation.md).

<div id="managing-distributed-tables">
  ## Gerenciando tabelas distribuídas
</div>

O ClickHouse pode gerenciar tabelas [distribuídas](../../engines/table-engines/special/distributed.md). Quando um usuário insere dados nessas tabelas, o ClickHouse primeiro cria uma fila com os dados que devem ser enviados aos nós do cluster e, em seguida, faz esse envio de forma assíncrona. Você pode gerenciar o processamento da fila com as consultas [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed) e [`START DISTRIBUTED SENDS`](#start-distributed-sends). Você também pode inserir dados distribuídos de forma síncrona com a configuração [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert).

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

Desativa a distribuição de dados em segundo plano ao inserir dados em tabelas distribuídas.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
Se [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) estiver habilitado (padrão), os dados ainda serão inseridos no shard local.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

Força o ClickHouse a enviar dados aos nós do cluster de forma síncrona. Se algum nó estiver indisponível, o ClickHouse lança uma exceção e interrompe a execução da consulta. Você pode repetir a consulta até que ela seja bem-sucedida, o que acontecerá quando todos os nós voltarem a ficar online.

Você também pode sobrescrever algumas configurações por meio da cláusula `SETTINGS`; isso pode ser útil para contornar limitações temporárias, como `max_concurrent_queries_for_all_users` ou `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
Cada bloco pendente é armazenado em disco com as configurações da consulta INSERT inicial; por isso, às vezes, você pode querer sobrescrever essas configurações.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

Ativa a distribuição de dados em segundo plano durante a inserção de dados em tabelas distribuídas.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

Fecha o socket e encerra normalmente as conexões existentes com o servidor na porta e com o protocolo especificados.

No entanto, se as configurações correspondentes do protocolo não tiverem sido especificadas na configuração do clickhouse-server, este comando não terá efeito.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* Se o modificador `CUSTOM 'protocol'` for especificado, o protocolo personalizado com o nome informado, definido na seção de protocolos da configuração do servidor, será interrompido.
* Se o modificador `QUERIES ALL [EXCEPT .. [,..]]` for especificado, todos os protocolos serão interrompidos, exceto os especificados na cláusula `EXCEPT`.
* Se o modificador `QUERIES DEFAULT [EXCEPT .. [,..]]` for especificado, todos os protocolos padrão serão interrompidos, exceto os especificados na cláusula `EXCEPT`.
* Se o modificador `QUERIES CUSTOM [EXCEPT .. [,..]]` for especificado, todos os protocolos personalizados serão interrompidos, exceto os especificados na cláusula `EXCEPT`.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

Permite estabelecer novas conexões nos protocolos especificados.

No entanto, se o servidor na porta e no protocolo especificados não tiver sido interrompido com o comando SYSTEM STOP LISTEN, este comando não terá efeito.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## Gerenciando tabelas MergeTree
</div>

O ClickHouse pode gerenciar processos em segundo plano nas tabelas [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

Permite interromper os merge em segundo plano de tabelas da família MergeTree:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
`DETACH / ATTACH` de uma tabela iniciará os merge em segundo plano dessa tabela, mesmo que os merge tenham sido interrompidos anteriormente para todas as tabelas MergeTree.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

Permite iniciar merges em segundo plano para tabelas da família MergeTree:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

Permite interromper a exclusão, em segundo plano, de dados antigos de acordo com a [expressão TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) para tabelas da família MergeTree:
Retorna `Ok.` mesmo que a tabela não exista ou não use o engine MergeTree. Retorna erro quando o banco de dados não existe:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

Permite iniciar a exclusão em segundo plano de dados antigos de acordo com a [expressão TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) para tabelas da família MergeTree:
Retorna `Ok.` mesmo se a tabela não existir. Retorna erro quando o banco de dados não existir:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

Permite interromper as operações em segundo plano de movimentação de dados de acordo com a [expressão TTL da tabela com a cláusula TO VOLUME ou TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) para tabelas da família MergeTree:
Retorna `Ok.` mesmo que a tabela não exista. Retorna erro quando o banco de dados não existe:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

Permite iniciar movimentações de dados em segundo plano de acordo com a [expressão TTL da tabela com as cláusulas TO VOLUME e TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) para tabelas da família MergeTree:
Retorna `Ok.` mesmo que a tabela não exista. Retorna erro quando o banco de dados não existe:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

Remove de todos os disks um backup congelado com o nome especificado. Veja mais sobre como descongelar partes individuais em [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/pt-BR/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

Aguarde até que todas as partes de dados de uma tabela carregadas assincronamente (partes de dados desatualizadas) sejam carregadas.

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## Gerenciamento de tabelas ReplicatedMergeTree
</div>

O ClickHouse pode gerenciar processos de replicação em segundo plano em tabelas [ReplicatedMergeTree](/pt-BR/engines/table-engines/mergetree-family/replication).

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

Permite interromper os fetches em segundo plano de partes inseridas em tabelas da família `ReplicatedMergeTree`:
Sempre retorna `Ok.`, independentemente do engine da tabela, mesmo que a tabela ou o banco de dados não existam.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

Permite iniciar fetches em segundo plano das partes inseridas para tabelas da família `ReplicatedMergeTree`:
Sempre retorna `Ok.` independentemente do engine da tabela, mesmo que a tabela ou o banco de dados não existam.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

Permite interromper os envios em segundo plano, para outras réplicas no cluster, de novas partes inseridas em tabelas da família `ReplicatedMergeTree`:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

Permite iniciar, em segundo plano, o envio para outras réplicas no cluster de novas partes inseridas em tabelas da família `ReplicatedMergeTree`:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

Permite interromper tarefas de fetch em segundo plano das filas de replicação armazenadas no ZooKeeper para tabelas da família `ReplicatedMergeTree`. Os possíveis tipos de tarefas em segundo plano são: merges, fetches, mutação, instruções DDL com cláusula ON CLUSTER:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

Permite iniciar tarefas de fetch em segundo plano a partir das filas de replicação armazenadas no ZooKeeper para tabelas da família `ReplicatedMergeTree`. Tipos possíveis de tarefas em segundo plano - merges, fetches, mutation, instruções DDL com a cláusula ON CLUSTER:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

Interrompe a leitura de novas entradas do log de replicação para a fila de replicação em uma tabela `ReplicatedMergeTree`.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

Cancela `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

Aguarde até que uma tabela `ReplicatedMergeTree` seja sincronizada com outras réplicas em um cluster, mas não por mais de `receive_timeout` segundos.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

Após executar esta instrução, `[db.]replicated_merge_tree_family_table_name` faz fetch de comandos do log replicado comum para sua própria fila de replicação, e então a consulta aguarda até que a réplica processe todos os comandos obtidos. Os seguintes modificadores são compatíveis:

* Com `IF EXISTS` (disponível desde a versão 25.6), a consulta não gerará erro se a tabela não existir. Isso é útil ao adicionar uma nova réplica a um cluster, quando ela já faz parte da configuração do cluster, mas ainda está em processo de criação e sincronização da tabela.
* Se um modificador `STRICT` for especificado, a consulta aguarda até que a fila de replicação fique vazia. A versão `STRICT` pode nunca ser concluída com sucesso se novas entradas aparecerem constantemente na fila de replicação.
* Se um modificador `LIGHTWEIGHT` for especificado, a consulta aguarda apenas que as entradas `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` e `DROP_PART` sejam processadas.
  Além disso, o modificador LIGHTWEIGHT oferece suporte a uma cláusula FROM &#39;srcReplicas&#39; opcional, em que &#39;srcReplicas&#39; é uma lista de nomes de réplicas de origem separados por vírgula. Essa extensão permite uma sincronização mais direcionada, concentrando-se apenas nas tarefas de replicação originadas das réplicas de origem especificadas.
* Se um modificador `PULL` for especificado, a consulta extrai novas entradas da fila de replicação do ZooKeeper, mas não aguarda que nada seja processado.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

Aguarda até que o [banco de dados replicado](/pt-BR/engines/database-engines/replicated) especificado aplique todas as alterações de esquema da fila de DDL do banco de dados.

**Sintaxe**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

Permite reinicializar o estado da sessão do ZooKeeper para a tabela `ReplicatedMergeTree`, comparar o estado atual com o ZooKeeper como fonte de verdade e adicionar tarefas à fila do ZooKeeper, se necessário.
A inicialização da fila de replicação com base nos dados do ZooKeeper ocorre da mesma forma que na instrução `ATTACH TABLE`. Por um curto período, a tabela ficará indisponível para quaisquer operações.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

Restaura uma réplica se os dados [possivelmente] estiverem presentes, mas os metadados do ZooKeeper tiverem sido perdidos.

Funciona apenas em tabelas `ReplicatedMergeTree` em modo somente leitura.

É possível executar essa consulta após:

* Perda da raiz `/` do ZooKeeper.
* Perda do caminho das réplicas `/replicas`.
* Perda do caminho de uma réplica específica `/replicas/replica_name/`.

A réplica anexa as partes encontradas localmente e envia informações sobre elas ao ZooKeeper.
As partes presentes em uma réplica antes da perda dos metadados não são buscadas novamente em outras réplicas, desde que não estejam desatualizadas (ou seja, restaurar a réplica não significa baixar novamente todos os dados pela rede).

:::note
As partes em qualquer estado são movidas para a pasta `detached/`. As partes que estavam ativas antes da perda dos dados (confirmadas) são anexadas.
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

Restaura uma réplica se os dados [possivelmente] ainda estiverem presentes, mas os metadados do ZooKeeper tiverem sido perdidos.

**Sintaxe**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Exemplo**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**Sintaxe**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Sintaxe alternativa:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Exemplo**

Criando uma tabela em vários servidores. Depois que os metadados da réplica no ZooKeeper forem perdidos, a tabela será anexada em modo somente leitura, já que os metadados estão ausentes. A última consulta precisa ser executada em cada réplica.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Outra forma:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

Permite reinicializar o estado das sessões do Zookeeper para todas as tabelas `ReplicatedMergeTree`; compara o estado atual com o Zookeeper como fonte da verdade e adiciona tarefas à fila do Zookeeper, se necessário

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

Permite limpar o cache do sistema de arquivos.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
É pesado demais e pode ser usado de forma indevida.
:::

Fará a chamada de sistema `sync`.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

Carrega as chaves primárias da tabela especificada ou de todas as tabelas.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

Descarrega as chaves primárias da tabela especificada ou de todas as tabelas.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## Gerenciamento de views materializadas atualizáveis
</div>

Comandos para controlar tarefas em segundo plano executadas por [views materializadas atualizáveis](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

Fique de olho em [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) ao usá-las.

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

Desativa a atualização periódica da view especificada ou de todas as views atualizáveis. Se houver uma atualização em andamento, ela também será cancelada.

Se a view estiver em um banco de dados Replicated ou Shared, `STOP VIEW` afeta apenas a réplica atual, enquanto `STOP REPLICATED VIEW` afeta todas as réplicas.

:::note
O estado de interrupção não persiste após reinicializações do servidor. Após uma reinicialização, as views retomarão os agendamentos de atualização configurados.
Em bancos de dados Replicated ou Shared, `SYSTEM STOP VIEW` afeta apenas a réplica atual. Use `SYSTEM STOP REPLICATED VIEW` para interromper as atualizações em todas as réplicas.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

Ativa a atualização periódica da view especificada ou de todas as views atualizáveis. Nenhuma atualização imediata é disparada.

Se a view estiver em um banco de dados Replicated ou Shared, `START VIEW` desfaz o efeito de `STOP VIEW`, e `START REPLICATED VIEW` desfaz o efeito de `STOP REPLICATED VIEW`. `START VIEW` também desfaz o efeito de `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

Desativa a atualização periódica da view especificada ou de todas as views atualizáveis.
Ao contrário de `SYSTEM STOP VIEW`, `SYSTEM PAUSE VIEW` não interrompe uma atualização que já esteja em andamento: a atualização em execução pode ser concluída, e apenas as atualizações subsequentes são impedidas.

Reverta com `SYSTEM START VIEW` ou `SYSTEM START VIEWS`.

:::note
O estado de pausa não persiste entre reinicializações do servidor. Após uma reinicialização, as views retomarão seus agendamentos de atualização configurados.
Em bancos de dados Replicated ou Shared, `SYSTEM PAUSE VIEW` afeta apenas a réplica atual.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

Executa uma atualização imediata, fora do agendamento, de uma determinada view.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

Aguarda a conclusão da atualização em execução. Se nenhuma atualização estiver em execução, retorna imediatamente. Se a tentativa de atualização mais recente falhar, relata um erro.

Pode ser usado logo após criar uma nova view materializada atualizável (sem a palavra-chave EMPTY) para aguardar a conclusão da atualização inicial.

Se a view estiver em um banco de dados Replicated ou Shared, e a atualização estiver em execução em outra réplica, aguarda a conclusão dessa atualização.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

Se houver um refresh em andamento da view especificada na réplica atual, interrompa e cancele a operação. Caso contrário, não faça nada.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

Bloqueia até que o arquivo especificado seja processado ou falhe permanentemente na tabela [S3Queue](../../engines/table-engines/integrations/s3queue.md) ou [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) especificada. Retorna imediatamente se o arquivo já tiver sido processado. Gera um erro se o arquivo tiver falhado permanentemente (todas as tentativas esgotadas).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```