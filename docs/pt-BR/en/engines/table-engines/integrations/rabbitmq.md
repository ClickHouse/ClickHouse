---
description: 'Este motor permite integrar o ClickHouse ao RabbitMQ.'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'Motor de tabela RabbitMQ'
doc_type: 'guide'
---

Este motor permite integrar o ClickHouse ao [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` permite:

* Publicar ou assinar fluxos de dados.
* Processar fluxos à medida que ficam disponíveis.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

Parâmetros obrigatórios:

* `rabbitmq_host_port` – host:port (por exemplo, `localhost:5672`).
* `rabbitmq_exchange_name` – nome da exchange do RabbitMQ.
* `rabbitmq_format` – formato da mensagem. Usa a mesma notação que a função SQL `FORMAT`, como `JSONEachRow`. Para mais informações, consulte a seção [Formatos](../../../interfaces/formats.md).

Parâmetros opcionais:

* `rabbitmq_exchange_type` – O tipo de exchange do RabbitMQ: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. Padrão: `fanout`.
* `rabbitmq_routing_key_list` – Uma lista de chaves de roteamento separadas por vírgulas.
* `rabbitmq_schema` – Parâmetro que deve ser usado se o formato exigir uma definição de esquema. Por exemplo, [Cap&#39;n Proto](https://capnproto.org/) exige o caminho para o arquivo de esquema e o nome do objeto raiz `schema.capnp:Message`.
* `rabbitmq_num_consumers` – O número de consumidores por tabela. Especifique mais consumidores se a taxa de transferência de um consumidor for insuficiente. Padrão: `1`
* `rabbitmq_num_queues` – Número total de filas. Aumentar esse número pode melhorar significativamente o desempenho. Padrão: `1`.
* `rabbitmq_queue_base` - Especifique um prefixo para nomes de fila. Os casos de uso dessa configuração são descritos abaixo.
* `rabbitmq_persistent` - Se definido como 1 (true), o modo de entrega da consulta de inserção será definido como 2 (marca as mensagens como &#39;persistent&#39;). Padrão: `0`.
* `rabbitmq_skip_broken_messages` – Tolerância do analisador de mensagens do RabbitMQ a mensagens incompatíveis com o esquema por bloco. Se `rabbitmq_skip_broken_messages = N`, o motor ignora *N* mensagens do RabbitMQ que não podem ser analisadas (uma mensagem equivale a uma linha de dados). Padrão: `0`.
* `rabbitmq_max_block_size` - Número de linhas coletadas antes de descarregar os dados do RabbitMQ. Padrão: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `rabbitmq_flush_interval_ms` - Timeout para descarregar os dados do RabbitMQ. Padrão: [stream&#95;flush&#95;interval&#95;ms](/pt-BR/operations/settings/settings#stream_flush_interval_ms).
* `rabbitmq_queue_settings_list` - permite definir configurações do RabbitMQ ao criar uma fila. Configurações disponíveis: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. A configuração `durable` é habilitada automaticamente para a fila.
* `rabbitmq_address` - Endereço para conexão. Use esta configuração ou `rabbitmq_host_port`.
* `rabbitmq_vhost` - vhost do RabbitMQ. Padrão: `'/'`.
* `rabbitmq_queue_consume` - Usa filas definidas pelo usuário e não faz nenhuma configuração do RabbitMQ: declaração de exchanges, filas e bindings. Padrão: `false`.
* `rabbitmq_username` - nome de usuário do RabbitMQ.
* `rabbitmq_password` - senha do RabbitMQ.
* `reject_unhandled_messages` - Rejeita mensagens (envia confirmação negativa ao RabbitMQ) em caso de erros. Essa configuração é habilitada automaticamente se houver um `x-dead-letter-exchange` definido em `rabbitmq_queue_settings_list`.
* `rabbitmq_commit_on_select` - Faz commit das mensagens quando uma consulta `select` é executada. Padrão: `false`.
* `rabbitmq_max_rows_per_message` — O número máximo de linhas gravadas em uma mensagem do RabbitMQ para formatos baseados em linha. Padrão: `1`.
* `rabbitmq_empty_queue_backoff_start_ms` — Um ponto inicial de backoff para reagendar a leitura se a fila do RabbitMQ estiver vazia.
* `rabbitmq_empty_queue_backoff_end_ms` — Um ponto final de backoff para reagendar a leitura se a fila do RabbitMQ estiver vazia.
* `rabbitmq_empty_queue_backoff_step_ms` — Um passo de backoff para reagendar a leitura se a fila do RabbitMQ estiver vazia.
* `rabbitmq_handle_error_mode` — Como tratar erros do motor RabbitMQ. Valores possíveis: default (a exceção será lançada se não conseguirmos analisar uma mensagem), stream (a mensagem da exceção e a mensagem bruta serão salvas nas colunas virtuais `_error` e `_raw_message`), dead&#95;letter&#95;queue (os dados relacionados ao erro serão salvos em system.dead&#95;letter&#95;queue).

<div id="ssl-connection">
  ### Conexão SSL
</div>

Use `rabbitmq_secure = 1` ou `amqps` no endereço de conexão: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
O comportamento padrão da biblioteca usada não é verificar se a conexão TLS criada é suficientemente segura. Se o certificado estiver expirado, for autoassinado, estiver ausente ou for inválido, a conexão simplesmente é permitida. Uma verificação mais rigorosa dos certificados poderá ser implementada no futuro.

As configurações de formato também podem ser adicionadas junto com as configurações relacionadas ao RabbitMQ.

Exemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

A configuração do servidor RabbitMQ deve ser adicionada no arquivo de configuração do ClickHouse.

Configuração necessária:

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Configuração adicional:

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## Descrição
</div>

`SELECT` não é particularmente útil para ler mensagens (exceto para depuração), porque cada mensagem pode ser lida apenas uma vez. É mais prático criar fluxos em tempo real usando [visões materializadas](../../../sql-reference/statements/create/view.md). Para fazer isso:

1. Use o motor para criar um consumidor do RabbitMQ e trate-o como um fluxo de dados.
2. Crie uma tabela com a estrutura desejada.
3. Crie uma visão materializada que converta os dados do motor e os insira em uma tabela criada anteriormente.

Quando a `MATERIALIZED VIEW` é associada ao motor, ela começa a coletar dados em segundo plano. Isso permite que você receba continuamente mensagens do RabbitMQ e as converta para o formato necessário usando `SELECT`.
Uma tabela do RabbitMQ pode ter quantas visões materializadas você quiser.

Os dados podem ser encaminhados com base em `rabbitmq_exchange_type` e na `rabbitmq_routing_key_list` especificada.
Não pode haver mais de uma exchange por tabela. Uma exchange pode ser compartilhada entre várias tabelas — isso permite o roteamento para várias tabelas ao mesmo tempo.

Opções de tipo de exchange:

* `direct` - O roteamento é baseado na correspondência exata das chaves. Exemplo de lista de chaves da tabela: `key1,key2,key3,key4,key5`; a chave da mensagem pode ser igual a qualquer uma delas.
* `fanout` - Roteamento para todas as tabelas (em que o nome da exchange é o mesmo), independentemente das chaves.
* `topic` - O roteamento é baseado em padrões com chaves separadas por ponto. Exemplos: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
* `headers` - O roteamento é baseado em correspondências `key=value`, com a configuração `x-match=all` ou `x-match=any`. Exemplo de lista de chaves da tabela: `x-match=all,format=logs,type=report,year=2020`.
* `consistent_hash` - Os dados são distribuídos uniformemente entre todas as tabelas vinculadas (em que o nome da exchange é o mesmo). Observe que esse tipo de exchange deve ser habilitado com o plugin do RabbitMQ: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

A configuração `rabbitmq_queue_base` pode ser usada nos seguintes casos:

* para permitir que tabelas diferentes compartilhem filas, de modo que vários consumidores possam ser registrados para as mesmas filas, o que proporciona melhor desempenho. Se você usar as configurações `rabbitmq_num_consumers` e/ou `rabbitmq_num_queues`, a correspondência exata das filas será obtida caso esses parâmetros sejam os mesmos.
* para conseguir retomar a leitura de determinadas filas duráveis quando nem todas as mensagens tiverem sido consumidas com sucesso. Para retomar o consumo de uma fila específica, defina seu nome na configuração `rabbitmq_queue_base` e não especifique `rabbitmq_num_consumers` nem `rabbitmq_num_queues` (o padrão é 1). Para retomar o consumo de todas as filas que foram declaradas para uma tabela específica, basta especificar as mesmas configurações: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. Por padrão, os nomes das filas serão exclusivos para as tabelas.
* para reutilizar filas, já que elas são declaradas como duráveis e não são excluídas automaticamente. (Podem ser excluídas por qualquer uma das ferramentas de CLI do RabbitMQ.)

Para melhorar o desempenho, as mensagens recebidas são agrupadas em blocos do tamanho de [max&#95;insert&#95;block&#95;size](/pt-BR/operations/settings/settings#max_insert_block_size). Se o bloco não tiver sido formado dentro de [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) milissegundos, os dados serão gravados na tabela independentemente de o bloco estar completo.

Se as configurações `rabbitmq_num_consumers` e/ou `rabbitmq_num_queues` forem especificadas junto com `rabbitmq_exchange_type`, então:

* o plugin `rabbitmq-consistent-hash-exchange` deve estar habilitado.
* a propriedade `message_id` das mensagens publicadas deve ser especificada (única para cada mensagem/lote).

Para a consulta INSERT, há metadados da mensagem que são adicionados a cada mensagem publicada: `messageID` e o sinalizador `republished` (true, se publicada mais de uma vez) — eles podem ser acessados por meio dos cabeçalhos da mensagem.

Não use a mesma tabela para inserts e visões materializadas.

Exemplo:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_exchange_name` - Nome do exchange do RabbitMQ. Tipo de dado: `String`.
* `_channel_id` - ChannelID no qual o consumidor que recebeu a mensagem foi declarado. Tipo de dado: `String`.
* `_delivery_tag` - DeliveryTag da mensagem recebida. Com escopo por canal. Tipo de dado: `UInt64`.
* `_redelivered` - flag `redelivered` da mensagem. Tipo de dado: `UInt8`.
* `_message_id` - messageID da mensagem recebida; não vazio se tiver sido definido no momento da publicação da mensagem. Tipo de dado: `String`.
* `_timestamp` - timestamp da mensagem recebida; não vazio se tiver sido definido no momento da publicação da mensagem. Tipo de dado: `UInt64`.

Colunas virtuais adicionais quando `rabbitmq_handle_error_mode='stream'`:

* `_raw_message` - Mensagem bruta que não pôde ser analisada com sucesso. Tipo de dado: `Nullable(String)`.
* `_error` - Mensagem de exceção gerada durante a falha na análise. Tipo de dado: `Nullable(String)`.

Nota: as colunas virtuais `_raw_message` e `_error` são preenchidas apenas em caso de exceção durante a análise; elas são sempre `NULL` quando a mensagem é analisada com sucesso.

<div id="caveats">
  ## Ressalvas
</div>

Embora seja possível especificar [expressões de coluna padrão](/pt-BR/sql-reference/statements/create/table.md/#default_values) (como `DEFAULT`, `MATERIALIZED`, `ALIAS`) na definição da tabela, elas serão ignoradas. Em vez disso, as colunas serão preenchidas com os valores padrão correspondentes aos respectivos tipos.

<div id="data-formats-support">
  ## Suporte a formatos de dados
</div>

O motor RabbitMQ é compatível com todos os [formatos](../../../interfaces/formats.md) compatíveis com o ClickHouse.
O número de linhas em uma mensagem do RabbitMQ depende de o formato ser baseado em linhas ou em blocos:

* Para formatos baseados em linhas, o número de linhas em uma mensagem do RabbitMQ pode ser controlado pela configuração `rabbitmq_max_rows_per_message`.
* Para formatos baseados em blocos, não é possível dividir um bloco em partes menores, mas o número de linhas em um bloco pode ser controlado pela configuração geral [max&#95;block&#95;size](/pt-BR/operations/settings/settings#max_block_size).