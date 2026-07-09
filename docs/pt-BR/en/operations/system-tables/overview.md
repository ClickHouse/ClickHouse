---
description: 'Visão geral do que são as tabelas de sistema e por que elas são úteis.'
keywords: ['tabelas de sistema', 'visão geral']
sidebar_label: 'Visão geral'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'Visão geral das tabelas de sistema'
doc_type: 'reference'
---

<div id="system-tables-introduction">
  ## Visão geral das tabelas de sistema
</div>

As tabelas de sistema fornecem informações sobre:

* Estados, processos e ambiente do servidor.
* Processos internos do servidor.
* Opções usadas quando o binário do ClickHouse foi compilado.

Tabelas de sistema:

* Localizadas no banco de dados `system`.
* Disponíveis apenas para leitura de dados.
* Não podem ser removidas nem alteradas, mas podem ser desanexadas.

A maioria das tabelas de sistema armazena seus dados na RAM. Um servidor ClickHouse cria essas tabelas de sistema na inicialização.

Diferentemente de outras tabelas de sistema, as tabelas de log do sistema [metric&#95;log](../../operations/system-tables/metric_log.md), [query&#95;log](../../operations/system-tables/query_log.md), [query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md), [trace&#95;log](../../operations/system-tables/trace_log.md), [part&#95;log](../../operations/system-tables/part_log.md), [crash&#95;log](../../operations/system-tables/crash_log.md), [text&#95;log](../../operations/system-tables/text_log.md) e [backup&#95;log](../../operations/system-tables/backup_log.md) usam o motor de tabela [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) e armazenam seus dados em um sistema de arquivos por padrão. Se você remover uma tabela de um sistema de arquivos, o servidor ClickHouse criará novamente uma tabela vazia na próxima gravação de dados. Se o schema da tabela de sistema mudar em um novo lançamento, o ClickHouse renomeará a tabela atual e criará uma nova.

As tabelas de log do sistema podem ser personalizadas criando um arquivo de configuração com o mesmo nome da tabela em `/etc/clickhouse-server/config.d/` ou definindo os elementos correspondentes em `/etc/clickhouse-server/config.xml`. Os elementos que podem ser personalizados são:

* `database`: banco de dados ao qual a tabela de log do sistema pertence. Esta opção está obsoleta no momento. Todas as tabelas de log do sistema ficam no banco de dados `system`.
* `table`: tabela na qual inserir dados.
* `partition_by`: especifica a expressão [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
* `ttl`: especifica a expressão [TTL](../../sql-reference/statements/alter/ttl.md) da tabela.
* `flush_interval_milliseconds`: intervalo de gravação dos dados no disco.
* `engine`: fornece a expressão completa do motor (começando com `ENGINE =` ) com parâmetros. Esta opção entra em conflito com `partition_by` e `ttl`. Se forem definidas juntas, o servidor gerará uma exceção e será encerrado.

Um exemplo:

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

Por padrão, o crescimento da tabela é ilimitado. Para controlar o tamanho de uma tabela, você pode usar as configurações de [TTL](/pt-BR/sql-reference/statements/alter/ttl) para remover registros de log obsoletos. Você também pode usar o recurso de particionamento em tabelas com o motor `MergeTree`.

<div id="system-tables-sources-of-system-metrics">
  ## Fontes de métricas do sistema
</div>

Para coletar métricas do sistema, o servidor ClickHouse usa:

* a capability `CAP_NET_ADMIN`.
* [procfs](https://en.wikipedia.org/wiki/Procfs) (somente no Linux).

**procfs**

Se o servidor ClickHouse não tiver a capability `CAP_NET_ADMIN`, ele tentará usar `ProcfsMetricsProvider` como alternativa. O `ProcfsMetricsProvider` permite coletar métricas do sistema por consulta (de CPU e E/S).

Se o procfs for compatível e estiver habilitado no sistema, o servidor ClickHouse coletará estas métricas:

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` fica desabilitada por padrão em kernels Linux a partir da versão 5.14.x.
Você pode habilitá-la usando `sudo sysctl kernel.task_delayacct=1` ou criando um arquivo `.conf` em `/etc/sysctl.d/` com `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## Tabelas de sistema no ClickHouse Cloud
</div>

No ClickHouse Cloud, as tabelas de sistema fornecem informações essenciais sobre o estado e o desempenho do serviço, assim como nas implantações autogerenciadas. Algumas tabelas de sistema funcionam em nível de cluster, especialmente aquelas que obtêm seus dados dos nós do Keeper, que gerenciam metadados distribuídos. Essas tabelas refletem o estado coletivo do cluster e devem apresentar consistência quando consultadas em nós individuais. Por exemplo, a [`parts`](/pt-BR/operations/system-tables/parts) deve ser consistente independentemente do nó em que for consultada:

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

Por outro lado, outras tabelas de sistema são específicas de cada nó, por exemplo, por serem em memória ou por persistirem seus dados usando o motor de tabela MergeTree. Isso é típico de dados como logs e métricas. Essa persistência garante que os dados históricos permaneçam disponíveis para análise. No entanto, essas tabelas específicas de nó são inerentemente exclusivas de cada nó.

Em geral, as seguintes regras podem ser aplicadas para determinar se uma tabela de sistema é específica de nó:

* Tabelas de sistema com sufixo `_log`.
* Tabelas de sistema que expõem métricas, por exemplo, `metrics`, `asynchronous_metrics`, `events`.
* Tabelas de sistema que expõem processos em andamento, por exemplo, `processes`, `merges`.

Além disso, novas versões de tabelas de sistema podem ser criadas como resultado de upgrade ou de alterações em seu esquema. Essas versões são nomeadas usando um sufixo numérico.

Por exemplo, considere as tabelas `system.query_log`, que contêm uma linha para cada consulta executada pelo nó:

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### Consultando múltiplas versões
</div>

Podemos consultar essas tabelas com a função [`merge`](/pt-BR/sql-reference/table-functions/merge). Por exemplo, a consulta abaixo identifica a consulta mais recente enviada ao nó de destino em cada tabela `query_log`:

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note Não confie no sufixo numérico para determinar a ordem
Embora o sufixo numérico das tabelas possa sugerir a ordem dos dados, nunca se deve confiar nele. Por esse motivo, sempre use a função de tabela merge combinada com um filtro de data ao consultar intervalos de datas específicos.
:::

É importante destacar que essas tabelas ainda são **locais em cada nó**.

<div id="querying-across-nodes">
  ### Consultando em todos os nós
</div>

Para ter uma visão abrangente de todo o cluster, os usuários podem usar a função [`clusterAllReplicas`](/pt-BR/sql-reference/table-functions/cluster) em combinação com a função `merge`. A função `clusterAllReplicas` permite consultar tabelas de sistema em todas as réplicas do cluster &quot;default&quot;, consolidando os dados específicos de cada nó em um resultado unificado. Quando combinada com a função `merge`, ela pode ser usada para acessar todos os dados de sistema de uma tabela específica em um cluster.

Essa abordagem é particularmente valiosa para monitoramento e debugging de operações em todo o cluster, garantindo que os usuários possam analisar com eficácia a integridade e o desempenho da sua implantação no ClickHouse Cloud.

:::note
O ClickHouse Cloud fornece clusters com múltiplas réplicas para redundância e failover. Isso viabiliza recursos como autoscaling dinâmico e upgrade sem downtime. Em um determinado momento, novos nós podem estar sendo adicionados ao cluster ou removidos dele. Para ignorar esses nós, adicione `SETTINGS skip_unavailable_shards = 1` às consultas que usam `clusterAllReplicas`, como mostrado abaixo.
:::

Por exemplo, considere a diferença ao consultar a tabela `query_log` — que muitas vezes é essencial para a análise.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### Consultando entre nós e versões
</div>

Devido ao versionamento das tabelas de sistema, isso ainda não representa todos os dados do cluster. Ao combinar o que foi mostrado acima com a função `merge`, obtemos um resultado preciso para o nosso intervalo de datas:

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Tabelas do sistema e uma visão dos componentes internos do ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* Blog: [Consultas essenciais para monitoramento - parte 1 - consultas INSERT](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* Blog: [Consultas essenciais para monitoramento - parte 2 - consultas SELECT](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)