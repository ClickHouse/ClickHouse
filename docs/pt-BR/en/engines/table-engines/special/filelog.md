---
description: 'Este motor permite processar arquivos de log da aplicação como um fluxo de
  registros.'
sidebar_label: 'FileLog'
sidebar_position: 160
slug: /engines/table-engines/special/filelog
title: 'Motor de tabela FileLog'
doc_type: 'reference'
---

Este motor permite processar arquivos de log da aplicação como um fluxo de registros.

`FileLog` permite que você:

* Inscreva-se em arquivos de log.
* Processe novos registros à medida que são acrescentados aos arquivos de log monitorados.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

Argumentos do motor:

* `path_to_logs` – Caminho dos arquivos de log a serem assinados. Pode ser o caminho para um diretório com arquivos de log ou para um único arquivo de log. Observe que o ClickHouse permite apenas caminhos dentro do diretório `user_files`.
* `format_name` - Formato do registro. Observe que o FileLog processa cada linha de um arquivo como um registro separado, e nem todos os formatos de dados são adequados para isso.

Parâmetros opcionais:

* `poll_timeout_ms` - Timeout de uma única operação de poll no arquivo de log. Padrão: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `poll_max_batch_size` — Quantidade máxima de registros obtidos em uma única operação de poll. Padrão: [max&#95;block&#95;size](/pt-BR/operations/settings/settings#max_block_size).
* `max_block_size` — Tamanho máximo do batch (em registros) para poll. Padrão: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `max_threads` - Número máximo de threads para analisar os arquivos; o padrão é 0, o que significa que o número será max(1, physical&#95;cpu&#95;cores / 4).
* `poll_directory_watch_events_backoff_init` - Valor inicial de sleep da thread de monitoramento do diretório. Padrão: `500`.
* `poll_directory_watch_events_backoff_max` - Valor máximo de sleep da thread de monitoramento do diretório. Padrão: `32000`.
* `poll_directory_watch_events_backoff_factor` - Velocidade do backoff, exponencial por padrão. Padrão: `2`.
* `handle_error_mode` — Como tratar erros no motor FileLog. Valores possíveis: default (a exceção será lançada se não conseguirmos analisar uma mensagem), stream (a mensagem de exceção e a mensagem bruta serão salvas nas colunas virtuais `_error` e `_raw_message`).

<div id="description">
  ## Descrição
</div>

Os registros recebidos são rastreados automaticamente, portanto cada registro em um arquivo de log é contado apenas uma vez.

`SELECT` não é particularmente útil para ler registros (exceto para depuração), porque cada registro só pode ser lido uma vez. É mais prático criar fluxos em tempo real usando [visões materializadas](../../../sql-reference/statements/create/view.md). Para fazer isso:

1. Use o motor para criar uma tabela FileLog e trate-a como um fluxo de dados.
2. Crie uma tabela com a estrutura desejada.
3. Crie uma visão materializada que converta os dados do motor e os insira em uma tabela criada anteriormente.

Quando a `MATERIALIZED VIEW` é associada ao motor, ela começa a coletar dados em segundo plano. Isso permite que você receba continuamente registros de arquivos de log e os converta para o formato necessário usando `SELECT`.
Uma tabela FileLog pode ter quantas visões materializadas você quiser; elas não leem dados da tabela diretamente, mas recebem novos registros (em blocos). Dessa forma, você pode gravar em várias tabelas com diferentes níveis de detalhe (com agrupamento - agregação e sem).

Exemplo:

```sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Para interromper o recebimento de dados dos streams ou alterar a lógica de conversão, desanexe a visão materializada:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Se você quiser alterar a tabela de destino com `ALTER`, recomendamos desativar a visão materializada para evitar discrepâncias entre a tabela de destino e os dados da view.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_filename` - Nome do arquivo de log. Tipo de dado: `LowCardinality(String)`.
* `_offset` - Deslocamento no arquivo de log. Tipo de dado: `UInt64`.

Colunas virtuais adicionais quando `handle_error_mode='stream'`:

* `_raw_record` - Registro bruto que não pôde ser analisado corretamente. Tipo de dado: `Nullable(String)`.
* `_error` - Mensagem da exceção ocorrida durante a falha de análise. Tipo de dado: `Nullable(String)`.

Observação: as colunas virtuais `_raw_record` e `_error` são preenchidas somente em caso de exceção durante a análise; elas são sempre `NULL` quando a mensagem é analisada com sucesso.