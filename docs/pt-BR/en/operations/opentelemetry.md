---
description: 'Guia para usar OpenTelemetry para rastreamento distribuído e coleta de métricas
  no ClickHouse'
sidebar_label: 'Rastreando o ClickHouse com OpenTelemetry'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'Rastreando o ClickHouse com OpenTelemetry'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) é um padrão aberto para coletar traces e métricas de aplicações distribuídas. O ClickHouse oferece algum suporte ao OpenTelemetry.

<div id="supplying-trace-context-to-clickhouse">
  ## Fornecendo contexto de rastreamento ao ClickHouse
</div>

O ClickHouse aceita cabeçalhos HTTP de contexto de rastreamento, conforme descrito na [recomendação do W3C](https://www.w3.org/TR/trace-context/). Ele também aceita contexto de rastreamento por meio de um protocolo nativo usado na comunicação entre servidores do ClickHouse ou entre o cliente e o servidor. Para testes manuais, cabeçalhos de contexto de rastreamento em conformidade com a recomendação Trace Context podem ser fornecidos ao `clickhouse-client` usando as flags `--opentelemetry-traceparent` e `--opentelemetry-tracestate`.

Se nenhum contexto de rastreamento pai for fornecido, ou se o contexto de rastreamento informado não estiver em conformidade com o padrão W3C mencionado acima, o ClickHouse poderá iniciar um novo trace, com a probabilidade controlada pela configuração [opentelemetry&#95;start&#95;trace&#95;probability](/pt-BR/operations/settings/settings#opentelemetry_start_trace_probability).

<div id="propagating-the-trace-context">
  ## Propagação do contexto de rastreamento
</div>

O contexto de rastreamento é propagado para serviços subsequentes nos seguintes casos:

* Consultas a servidores ClickHouse remotos, por exemplo, ao usar o motor de tabela [Distributed](../engines/table-engines/special/distributed.md).

* Função de tabela [url](../sql-reference/table-functions/url.md). As informações de contexto de rastreamento são enviadas em cabeçalhos HTTP.

<div id="tracing-clickhouse-keeper-requests">
  ## Rastreamento de requisições do ClickHouse Keeper
</div>

O ClickHouse oferece rastreamento via OpenTelemetry para requisições do [ClickHouse Keeper](../guides/sre/keeper/index.md) (serviço de coordenação compatível com o ZooKeeper). Esse recurso fornece visibilidade detalhada do ciclo de vida das operações do Keeper, desde o envio da requisição pelo cliente até o processamento no servidor.

<div id="enabling-keeper-tracing">
  ### Habilitando o rastreamento do Keeper
</div>

Para habilitar o rastreamento das requisições do Keeper, configure as seguintes opções na configuração do cliente ZooKeeper/Keeper:

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Tipos de span do Keeper
</div>

Quando o tracing está habilitado, o ClickHouse cria spans para operações do Keeper tanto no lado do cliente quanto no lado do servidor:

**Spans do lado do cliente:**

* `zookeeper.create` — Criar um novo node
* `zookeeper.get` — Obter dados do node
* `zookeeper.set` — Definir dados do node
* `zookeeper.remove` — Remover um node
* `zookeeper.list` — Listar child nodes
* `zookeeper.exists` — Verificar se um node existe
* `zookeeper.multi` — Executar várias operações de forma atômica
* `zookeeper.client.requests_queue` — Tempo gasto enfileirando requests antes do envio

**Spans do lado do servidor (Keeper):**

* `keeper.receive_request` — Recebimento e parsing da request do client
* `keeper.dispatcher.requests_queue` — Enfileiramento de request no dispatcher
* `keeper.write.pre_commit` — Pré-processamento de write requests antes do Raft commit
* `keeper.write.commit` — Processamento de write requests após o Raft commit
* `keeper.read.wait_for_write` — Read requests aguardando writes dependentes
* `keeper.read.process` — Processamento de read requests
* `keeper.dispatcher.responses_queue` — Enfileiramento de response no dispatcher
* `keeper.send_response` — Envio da response ao client

<div id="sampling-and-performance">
  ### Amostragem e desempenho
</div>

Para gerenciar a sobrecarga do rastreamento, o Keeper implementa amostragem dinâmica. A taxa de amostragem é ajustada automaticamente entre 1/10.000 e 1/10 com base no tamanho da requisição. Todas as requisições (amostradas e não amostradas) têm suas durações registradas em métricas do tipo histograma para monitoramento de desempenho.

<div id="tracing-the-clickhouse-itself">
  ## Rastreando o próprio ClickHouse
</div>

O ClickHouse cria `trace spans` para cada consulta e para algumas etapas da execução da consulta, como o planejamento da consulta ou consultas distribuídas.

Para ser útil, as informações de rastreamento precisam ser exportadas para um sistema de monitoramento compatível com OpenTelemetry, como [Jaeger](https://jaegertracing.io/) ou [Prometheus](https://prometheus.io/). O ClickHouse evita depender de um sistema de monitoramento específico e, em vez disso, apenas disponibiliza os dados de rastreamento por meio de uma tabela de sistema. As informações de `trace span` do OpenTelemetry [exigidas pelo padrão](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) são armazenadas na tabela [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md).

A tabela deve estar habilitada na configuração do servidor; consulte o elemento `opentelemetry_span_log` no arquivo de configuração padrão `config.xml`. Ela é habilitada por padrão.

As tags ou atributos são salvos em dois arrays paralelos, contendo as chaves e os valores. Use [ARRAY JOIN](../sql-reference/statements/select/array-join.md) para trabalhar com eles.

<div id="log-query-settings">
  ## Configurações de log da consulta
</div>

A configuração [log&#95;query&#95;settings](settings/settings.md) permite registrar alterações nas configurações da consulta durante sua execução. Quando habilitada, qualquer modificação feita nas configurações da consulta será registrada no log de span do OpenTelemetry. Esse recurso é particularmente útil em ambientes de produção para rastrear alterações de configuração que possam afetar o desempenho da consulta.

<div id="integration-with-monitoring-systems">
  ## Integração com sistemas de monitoramento
</div>

No momento, não existe uma ferramenta pronta que exporte os dados de rastreamento do ClickHouse para um sistema de monitoramento.

Para testes, é possível configurar a exportação usando uma visão materializada com o motor [URL](../engines/table-engines/special/url.md) sobre a tabela [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md), que enviaria os dados de log recebidos para um endpoint HTTP de um collector de traces. Por exemplo, para enviar os dados mínimos de span para uma instância do Zipkin em execução em `http://localhost:9411`, no formato JSON v2 do Zipkin:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

Em caso de erro, a parte dos dados de log em que o erro ocorreu será perdida sem aviso. Verifique o log do servidor em busca de mensagens de erro se os dados não chegarem.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Criando uma solução de observabilidade com ClickHouse - Parte 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)