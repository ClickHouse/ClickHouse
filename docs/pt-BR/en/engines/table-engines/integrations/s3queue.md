---
description: 'Este motor oferece integração com o ecossistema do Amazon S3 e permite
  importações em streaming. Semelhante aos motores Kafka e RabbitMQ, mas com recursos
  específicos do S3.'
sidebar_label: 'S3Queue'
sidebar_position: 181
slug: /engines/table-engines/integrations/s3queue
title: 'Motor de tabela S3Queue'
doc_type: 'referência'
---

import ScalePlanFeatureBadge from '@theme/badges/ScalePlanFeatureBadge'

<div id="s3queue-table-engine">
  # motor de tabela S3Queue
</div>

Este motor fornece integração com o ecossistema do [Amazon S3](https://aws.amazon.com/s3/) e permite importação por streaming. Esse motor é semelhante aos motores [Kafka](../../../engines/table-engines/integrations/kafka.md) e [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md), mas oferece recursos específicos do S3.

É importante entender esta observação do [PR original da implementação do S3Queue](https://github.com/ClickHouse/ClickHouse/pull/49086/files#diff-e1106769c9c8fbe48dd84f18310ef1a250f2c248800fde97586b3104e9cd6af8R183): quando a `MATERIALIZED VIEW` é associada ao motor, o motor de tabela S3Queue começa a coletar dados em segundo plano.

<div id="creating-a-table">
  ## Criar tabela
</div>

```sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression], [headers], [extra_credentials])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 10,]
    [processing_threads_num = 16,]
    [parallel_inserts = false,]
    [enable_logging_to_queue_log = true,]
    [last_processed_path = "",]
    [tracked_files_limit = 1000,]
    [tracked_file_ttl_sec = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 600000,]
    [polling_backoff_ms = 30000,]
    [cleanup_interval_min_ms = 60000,]
    [cleanup_interval_max_ms = 60000,]
    [buckets = 0,]
    [list_objects_batch_size = 1000,]
    [enable_hash_ring_filtering = 0,]
    [max_processed_files_before_commit = 100,]
    [max_processed_rows_before_commit = 0,]
    [max_processed_bytes_before_commit = 0,]
    [max_processing_time_sec_before_commit = 0,]
```

:::warning
Antes da versão `24.7`, é necessário usar o prefixo `s3queue_` em todas as configurações, exceto `mode`, `after_processing` e `keeper_path`.
:::

**Parâmetros do motor**

Os parâmetros de `S3Queue` são os mesmos compatíveis com o motor de tabela `S3`. Consulte a seção de parâmetros [aqui](../../../engines/table-engines/integrations/s3.md#parameters).

**Exemplo**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

Usando named collections:

```xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

<div id="settings">
  ## Configurações
</div>

Para obter uma lista das configurações definidas para a tabela, use a tabela `system.s3_queue_settings`. Disponível a partir da versão `24.10`.

:::note Nomes das configurações (24.7+)
A partir da versão 24.7, as configurações do S3Queue podem ser especificadas com ou sem o prefixo `s3queue_`:

* **Sintaxe moderna** (24.7+): `processing_threads_num`, `tracked_file_ttl_sec` etc.
* **Sintaxe legada** (todas as versões): `s3queue_processing_threads_num`, `s3queue_tracked_file_ttl_sec` etc.

As duas formas são compatíveis a partir da versão 24.7. Os exemplos nesta página usam a sintaxe moderna, sem prefixo.
:::

<div id="mode">
  ### Modo
</div>

Valores possíveis:

* unordered — No modo unordered, o conjunto de todos os arquivos já processados é rastreado por meio de nós persistentes no ZooKeeper.
* ordered — No modo ordered, os arquivos são processados em ordem lexicográfica. Isso significa que, se um arquivo chamado &#39;BBB&#39; foi processado em algum momento e, posteriormente, um arquivo chamado &#39;AA&#39; for adicionado ao bucket, ele será ignorado. Somente o nome máximo (em sentido lexicográfico) do arquivo consumido com sucesso e os nomes dos arquivos que serão processados novamente após uma tentativa de carregamento malsucedida são armazenados no ZooKeeper.

Valor padrão: `ordered` em versões anteriores à 24.6. A partir da 24.6, não há valor padrão, e a configuração passa a precisar ser especificada manualmente. Para tabelas criadas em versões anteriores, o valor padrão permanecerá `Ordered` por compatibilidade.

<div id="after_processing">
  ### `after_processing`
</div>

Como tratar o arquivo após o processamento bem-sucedido.

Valores possíveis:

* keep.
* delete.
* move.
* tag.

Valor padrão: `keep`.

`move` exige configurações adicionais. No caso de uma movimentação dentro do mesmo bucket, um novo prefixo de caminho deve ser fornecido como `after_processing_move_prefix`.

A movimentação para outro bucket do S3 exige o URI do bucket de destino como `after_processing_move_uri` e as credenciais do S3 como `after_processing_move_access_key_id` e `after_processing_move_secret_access_key`.

Exemplo:

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_retries = 20,
    after_processing_move_prefix = 'dst_prefix',
    after_processing_move_uri = 'https://clickhouse-public-datasets.s3.amazonaws.com/dst-bucket',
    after_processing_move_access_key_id = 'test',
    after_processing_move_secret_access_key = 'test';
```

Para mover de um contêiner do Azure para outro, é necessário informar a connection string do Blob Storage em `after_processing_move_connection_string` e o nome do contêiner em `after_processing_move_container`. Veja [as configurações do AzureQueue](../../../engines/table-engines/integrations/azure-queue.md#settings).

A marcação exige que a chave e o valor da tag sejam fornecidos como `after_processing_tag_key` e `after_processing_tag_value`.

<div id="after_processing_retries">
  ### `after_processing_retries`
</div>

Número de novas tentativas para a ação solicitada após o processamento, antes de desistir.

Possíveis valores:

* Inteiro não negativo.

Valor padrão: `10`.

<div id="after_processing_move_access_key_id">
  ### `after_processing_move_access_key_id`
</div>

ID da chave de acesso do bucket do S3 para o qual mover arquivos processados com sucesso, se o destino for outro bucket do S3.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="after_processing_move_prefix">
  ### `after_processing_move_prefix`
</div>

Prefixo de caminho para o qual os arquivos processados com sucesso serão movidos. Aplica-se a ambos os casos: mover dentro do mesmo bucket e para outro bucket.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="after_processing_move_preserve_path">
  ### `after_processing_move_preserve_path`
</div>

Se `true`, o caminho completo do objeto de origem é anexado a `after_processing_move_prefix` ao mover um arquivo processado com sucesso, para que a estrutura de diretórios de origem sob o bucket seja preservada no destino. Se `false`, apenas o nome do arquivo é usado, e a estrutura de diretórios de origem é nivelada.

Valores possíveis:

* `true` / `false`.

Valor padrão: `false`.

<div id="after_processing_move_secret_access_key">
  ### `after_processing_move_secret_access_key`
</div>

Chave secreta de acesso do bucket do S3 para o qual os arquivos processados com sucesso serão movidos, caso o destino seja outro bucket do S3.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="after_processing_move_uri">
  ### `after_processing_move_uri`
</div>

URI do bucket do S3 para o qual os arquivos processados com sucesso serão movidos, se o destino for outro bucket do S3.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="after_processing_tag_key">
  ### `after_processing_tag_key`
</div>

Chave da tag usada para marcar arquivos processados com sucesso, se `after_processing='tag'`.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="after_processing_tag_value">
  ### `after_processing_tag_value`
</div>

Valor da tag a ser aplicado aos arquivos processados com sucesso, se `after_processing='tag'`.

Valores possíveis:

* String.

Valor padrão: string vazia.

<div id="keeper_path">
  ### `keeper_path`
</div>

Caminho para os metadados da fila no ZooKeeper. Se não for especificado explicitamente, o ClickHouse constrói o caminho a partir de `s3queue_default_zookeeper_path`, do UUID do banco de dados e do UUID da tabela. Valores absolutos (que começam com `/`) são usados tal como fornecidos, enquanto valores relativos são acrescentados ao prefixo configurado. Macros como `{database}` ou `{uuid}` são expandidas antes de o motor se conectar ao ZooKeeper.

Para apontar para um cluster ZooKeeper auxiliar, adicione ao valor o nome configurado como prefixo, por exemplo `analytics_keeper:/clickhouse/queue/orders`. O nome deve existir em `<auxiliary_zookeepers>`; caso contrário, o motor informa `Unknown auxiliary ZooKeeper name ...`. A string completa (incluindo o prefixo) é preservada em `SHOW CREATE TABLE` para que a instrução possa ser replicada literalmente.

Valores possíveis:

* String.

Valor padrão: `/`.

<div id="loading_retries">
  ### `loading_retries`
</div>

Tenta carregar o arquivo novamente até o número de vezes especificado.
Valores possíveis:

* Inteiro não negativo.

Valor padrão: `10`.

<div id="processing_threads_num">
  ### `processing_threads_num`
</div>

Número de threads para processamento. Aplica-se apenas ao modo `Unordered`.

Valor padrão: número de CPUs ou 16.

<div id="parallel_inserts">
  ### `parallel_inserts`
</div>

Por padrão, `processing_threads_num` produzirá um `INSERT`, então ele apenas fará download dos arquivos e o parse em várias threads.
Mas isso limita o paralelismo, portanto, para obter melhor throughput, use `parallel_inserts=true`; isso permitirá inserir dados em paralelo (mas tenha em mente que isso resultará em um número maior de partes de dados geradas para a família MergeTree).

:::note
Os `INSERT`s serão gerados de acordo com as configurações de `max_process*_before_commit`.
:::

Valor padrão: `false`.

<div id="enable_logging_to_queue_log">
  ### `enable_logging_to_queue_log`
</div>

Ativa o logging em `system.s3queue_log`.

Valor padrão: `1`.

<div id="polling_min_timeout_ms">
  ### `polling_min_timeout_ms`
</div>

Especifica o tempo mínimo, em milissegundos, que ClickHouse aguarda antes de fazer a próxima tentativa de polling.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `1000`.

<div id="polling_max_timeout_ms">
  ### `polling_max_timeout_ms`
</div>

Define o tempo máximo, em milissegundos, que o ClickHouse aguarda antes de iniciar a próxima tentativa de polling.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `600000`.

<div id="polling_backoff_ms">
  ### `polling_backoff_ms`
</div>

Determina o tempo de espera adicional acrescentado ao intervalo de polling anterior quando nenhum arquivo novo é encontrado. O próximo poll ocorre após a soma do intervalo anterior com esse valor de backoff, ou o intervalo máximo, o que for menor.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `30000`.

<div id="tracked_files_limit">
  ### `tracked_files_limit`
</div>

Permite limitar o número de nós do ZooKeeper se o modo &#39;unordered&#39; for usado; não tem efeito no modo &#39;ordered&#39;.
Se o limite for atingido, os arquivos processados mais antigos serão excluídos do nó do ZooKeeper e processados novamente.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `1000`.

<div id="tracked_file_ttl_sec">
  ### `tracked_file_ttl_sec`
</div>

Número máximo de segundos para armazenar arquivos processados no nó do ZooKeeper (mantidos para sempre por padrão) no modo &#39;unordered&#39;; não tem efeito no modo &#39;ordered&#39;.
Após o número especificado de segundos, o arquivo será importado novamente.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `0`.

<div id="cleanup_interval_min_ms">
  ### `cleanup_interval_min_ms`
</div>

Para o modo &#39;Ordered&#39;. Define um limite mínimo para o intervalo de reagendamento de uma tarefa em segundo plano, responsável por manter o TTL dos arquivos rastreados e o limite máximo de arquivos rastreados.

Valor padrão: `60000`.

<div id="cleanup_interval_max_ms">
  ### `cleanup_interval_max_ms`
</div>

Para o modo &#39;Ordered&#39;. Define um limite máximo para o intervalo de reagendamento de uma tarefa em segundo plano, responsável por gerenciar o TTL dos arquivos rastreados e o conjunto máximo de arquivos rastreados.

Valor padrão: `60000`.

<div id="buckets">
  ### `buckets`
</div>

Para o modo &#39;Ordered&#39;. Disponível desde a versão `24.6`. Se houver várias réplicas da tabela S3Queue, cada uma operando com o mesmo diretório de metadados no Keeper, o valor de `buckets` deve ser no mínimo igual ao número de réplicas. Se a configuração `processing_threads` também for usada, faz sentido aumentar ainda mais o valor da configuração `buckets`, pois ela define o paralelismo real do processamento do `S3Queue`.

<div id="use_persistent_processing_nodes">
  ### `use_persistent_processing_nodes`
</div>

Por padrão, a tabela S3Queue sempre usou nós de processamento efêmeros, o que podia levar à duplicação de dados caso a sessão do ZooKeeper expirasse antes que a S3Queue fizesse commit dos arquivos processados no ZooKeeper, mas depois de o processamento já ter sido iniciado. Essa configuração força o servidor a eliminar a possibilidade de duplicatas em caso de expiração da sessão do Keeper.

<div id="persistent_processing_node_ttl_seconds">
  ### `persistent_processing_node_ttl_seconds`
</div>

Em caso de encerramento inesperado do servidor, é possível que, se `use_persistent_processing_nodes` estiver habilitado, alguns nós de processamento não sejam removidos. Essa configuração define por quanto tempo esses nós de processamento podem ser removidos com segurança. O mesmo TTL também é usado para o bloqueio do bucket no modo `Ordered`, que pode ser mantido por mais tempo do que um único nó de processamento, portanto o valor também deve levar isso em consideração.

Valor padrão: `21600` (6 horas).

<div id="s3-settings">
  ## Configurações relacionadas ao S3
</div>

Este motor oferece suporte a todas as configurações relacionadas ao S3. Para mais informações sobre as configurações do S3, consulte [aqui](../../../engines/table-engines/integrations/s3.md).

<div id="s3-role-based-access">
  ## Acesso baseado em função para S3
</div>

<ScalePlanFeatureBadge feature="S3 Role-Based Access" />

O mecanismo de tabela S3Queue oferece suporte a acesso baseado em função.
Consulte a documentação [aqui](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração de uma função para acessar seu bucket.

Depois que a função estiver configurada, um `roleARN` poderá ser informado por meio do parâmetro `extra_credentials`, conforme mostrado abaixo:

```sql
CREATE TABLE s3_table
(
    ts DateTime,
    value UInt64
)
ENGINE = S3Queue(
                'https://<your_bucket>/*.csv',
                extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/<your_role>')
                ,'CSV')
SETTINGS
    ...
```

<div id="ordered-mode">
  ## Modo ordered do S3Queue
</div>

O modo de processamento `S3Queue` permite armazenar menos metadados no ZooKeeper, mas tem uma limitação: os arquivos adicionados mais tarde precisam ter nomes alfanumericamente maiores.

O modo `ordered` do `S3Queue`, assim como o `unordered`, oferece suporte à configuração `(s3queue_)processing_threads_num` (o prefixo `s3queue_` é opcional), que permite controlar o número de threads que farão localmente, no servidor, o processamento de arquivos `S3`.

Para o modo `ordered` sem particionamento, o ClickHouse pode retomar a listagem do S3 a partir da última chave processada para evitar listar novamente todo o histórico do prefixo. No modo ordered com buckets, o ponto de retomada é escolhido de forma conservadora como a menor chave processada entre todos os buckets, para evitar pular arquivos ainda não processados.
Essa otimização de retomada da listagem é usada apenas para filas com backend em S3 no modo ordered sem particionamento (não para AzureQueue nem quando `partitioning_mode` está definido).
Além disso, o modo `ordered` também introduz outra configuração chamada `(s3queue_)buckets`, que significa &quot;threads lógicas&quot;. Isso significa que, em um cenário distribuído, quando há vários servidores com réplicas da tabela `S3Queue`, essa configuração define o número de unidades de processamento. Por exemplo, cada thread de processamento em cada réplica de `S3Queue` tentará bloquear um determinado `bucket` para processamento; cada `bucket` é atribuído a determinados arquivos com base no hash do nome do arquivo. Portanto, em um cenário distribuído, é altamente recomendável que a configuração `(s3queue_)buckets` seja pelo menos igual ao número de réplicas, ou maior. Não há problema em ter um número de buckets maior que o número de réplicas. O cenário ideal é que a configuração `(s3queue_)buckets` seja igual ao produto de `number_of_replicas` por `(s3queue_)processing_threads_num`.
A configuração `(s3queue_)processing_threads_num` não é recomendada para uso antes da versão `24.6`.
A configuração `(s3queue_)buckets` está disponível a partir da versão `24.6`.

<div id="select">
  ## SELECT no motor de tabela S3Queue
</div>

Consultas `SELECT` são proibidas por padrão em tabelas S3Queue. Isso segue o padrão comum de fila, em que os dados são lidos uma vez e depois removidos da fila. `SELECT` é proibido para evitar perda acidental de dados.
No entanto, em alguns casos isso pode ser útil. Para isso, você precisa definir a configuração `stream_like_engine_allow_direct_select` como `True`.
O motor S3Queue tem uma configuração especial para consultas `SELECT`: `commit_on_select`. Defina-a como `False` para preservar os dados na fila após a leitura, ou como `True` para removê-los.

<div id="description">
  ## Descrição
</div>

`SELECT` não é particularmente útil para importação por streaming (exceto para depuração), porque cada arquivo pode ser importado apenas uma vez. É mais prático criar fluxos em tempo real usando [visões materializadas](../../../sql-reference/statements/create/view.md). Para fazer isso:

1. Use o motor para criar uma tabela que consuma do caminho especificado no S3 e trate-a como um fluxo de dados.
2. Crie uma tabela com a estrutura desejada.
3. Crie uma visão materializada que converta os dados do motor e os insira em uma tabela criada anteriormente.

Quando a `MATERIALIZED VIEW` é vinculada ao motor, ela começa a coletar dados em segundo plano.

Exemplo:

```sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo.
* `_file` — Nome do arquivo.
* `_size` — Tamanho do arquivo.
* `_time` — Momento de criação do arquivo.

Para mais informações sobre colunas virtuais, consulte [aqui](../../../engines/table-engines/index.md#table_engines-virtual_columns).

<div id="wildcards-in-path">
  ## Curingas no caminho
</div>

O argumento `path` pode especificar vários arquivos usando curingas no estilo do bash. Para ser processado, o arquivo deve existir e corresponder ao padrão do caminho completo. A listagem de arquivos é determinada durante o `SELECT` (não no momento do `CREATE`).

* `*` — Substitui qualquer quantidade de caracteres, exceto `/`, incluindo a string vazia.
* `**` — Substitui qualquer quantidade de caracteres, incluindo `/`, incluindo a string vazia.
* `?` — Substitui qualquer caractere individual.
* `{some_string,another_string,yet_another_one}` — Substitui qualquer uma das strings `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Substitui qualquer número no intervalo de N a M, incluindo ambos os limites. N e M podem ter zeros à esquerda, por exemplo `000..078`.

Construções com `{}` são semelhantes à função de tabela [remote](../../../sql-reference/table-functions/remote.md).

<div id="limitations">
  ## Limitações
</div>

1. Linhas duplicadas podem ocorrer como resultado de:

* uma exceção ocorrer durante o parsing no meio do processamento do arquivo, e as tentativas de repetição estiverem habilitadas via `s3queue_loading_retries`;

* o `S3Queue` estar configurado em vários servidores apontando para o mesmo caminho no ZooKeeper, e a sessão do Keeper expirar antes que um servidor consiga fazer commit do arquivo processado, o que pode levar outro servidor a assumir o processamento do arquivo, que pode já ter sido processado parcial ou totalmente pelo primeiro servidor; no entanto, isso deixou de ser verdade desde a versão 25.8 se `use_persistent_processing_nodes = 1`.

* encerramento anormal do servidor.

2. Se o `S3Queue` estiver configurado em vários servidores apontando para o mesmo caminho no ZooKeeper e o modo `Ordered` for usado, então `s3queue_loading_retries` não funcionará. Isso será corrigido em breve.

<div id="introspection">
  ## Introspecção
</div>

Para introspecção, use a tabela sem estado `system.s3queue_metadata_cache` e a tabela persistente `system.s3queue_log`.

1. `system.s3queue_metadata_cache`. Esta tabela não é persistente e mostra o estado em memória do `S3Queue`: quais arquivos estão sendo processados no momento e quais já foram processados ou falharam.

```sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_metadata_cache
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Exemplo:

```sql

SELECT *
FROM system.s3queue_metadata_cache

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. `system.s3queue_log`. Tabela persistente. Contém as mesmas informações que `system.s3queue_metadata_cache`, mas para arquivos `processed` e `failed`.

A tabela tem a seguinte estrutura:

```sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Para usar `system.s3queue_log`, defina a configuração correspondente no arquivo de configuração do servidor:

```xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

Exemplo:

```sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```