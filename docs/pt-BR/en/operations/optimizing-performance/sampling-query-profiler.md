---
description: 'Documentação da ferramenta de profiler de consultas por amostragem no ClickHouse'
sidebar_label: 'Perfilamento de consultas'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'Profiler de consultas por amostragem'
doc_type: 'referência'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # profiler de consultas por amostragem
</div>

O ClickHouse executa um profiler por amostragem que permite analisar a execução de consultas.
Usando o profiler, você pode encontrar as rotinas do código-fonte mais usadas durante a execução da consulta.
Você pode rastrear o tempo de CPU e o tempo de relógio gasto, incluindo o tempo ocioso.

O profiler de consultas é habilitado automaticamente no ClickHouse Cloud.
A consulta de exemplo a seguir encontra os stack traces mais frequentes de uma consulta analisada pelo profiler, com nomes de função resolvidos e localizações no código-fonte:

:::tip
Substitua o valor de `query_id` pelo ID da consulta que você deseja analisar com o profiler.
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    No ClickHouse Cloud, você pode obter o ID da consulta clicando em **&quot;...&quot;** no canto direito da barra acima da tabela de resultados da consulta (ao lado do seletor de tabela/gráfico). Isso abre um menu de contexto no qual você pode clicar em **&quot;Copy query ID&quot;**.

    Use `clusterAllReplicas(default, system.trace_log)` para selecionar dados de todos os nós do cluster:

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="Autogerenciado">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## Usando o profiler de consultas em implantações autogerenciadas
</div>

Em implantações autogerenciadas, para usar o profiler de consultas, siga as etapas abaixo:

<VerticalStepper headerLevel="h3">
  ### Instale o ClickHouse com informações de depuração

  Instale o pacote `clickhouse-common-static-dbg`:

  1. Siga as instruções da etapa [&quot;Configurar o repositório Debian&quot;](/pt-BR/install/debian_ubuntu#setup-the-debian-repository)
  2. Execute `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` para instalar os arquivos binários compilados do ClickHouse com informações de depuração
  3. Execute `sudo service clickhouse-server start` para iniciar o servidor
  4. Execute `clickhouse-client`. Os símbolos de depuração de `clickhouse-common-static-dbg` serão carregados automaticamente pelo servidor — você não precisa fazer nada especial para habilitá-los

  ### Verifique a configuração do servidor

  Certifique-se de que a seção [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) do seu [arquivo de configuração do servidor](/pt-BR/operations/configuration-files) esteja configurada. Ela vem habilitada por padrão:

  ```xml
  <!-- Trace log. Armazena stack traces coletados pelos profilers de consulta.
       Consulte as configurações query_profiler_real_time_period_ns e query_profiler_cpu_time_period_ns. -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- Indica se os logs devem ser gravados no disco em caso de falha -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  Esta seção configura a tabela de sistema [trace&#95;log](/pt-BR/operations/system-tables/trace_log), que contém os resultados do profiler.
  Lembre-se de que os dados dessa tabela são válidos apenas para um servidor em execução.
  Após a reinicialização do servidor, o ClickHouse não limpa a tabela, e todos os endereços de memória virtual armazenados podem se tornar inválidos.

  ### Configure os timers de profiling

  Configure as settings [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) ou [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns).
  Ambas as settings podem ser usadas simultaneamente.

  Essas settings permitem configurar os timers do profiler.
  Como são configurações de sessão, você pode definir diferentes frequências de amostragem para o servidor inteiro, usuários individuais ou perfis de usuário, para sua sessão interativa e para cada consulta individual.

  A frequência de amostragem padrão é de uma amostra por segundo, e tanto os timers de CPU quanto os de tempo real vêm habilitados.
  Essa frequência permite coletar informações suficientes sobre seu cluster ClickHouse sem afetar o desempenho do servidor.
  Se você precisar analisar o perfil de cada consulta individualmente, use uma frequência de amostragem mais alta.

  ### Analise a tabela de sistema `trace_log`

  Para analisar a tabela de sistema `trace_log`, habilite as introspection functions com a setting [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions):

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  Por motivos de segurança, as introspection functions vêm desabilitadas por padrão
  :::

  Use as [introspection functions](../../sql-reference/functions/introspection.md) `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` e `demangle` para obter os nomes das funções e suas posições no código do ClickHouse.
  Para obter um perfil de alguma consulta, você precisa agregar dados da tabela `trace_log`.
  Você pode agregar dados por funções individuais ou por stack traces completos.

  :::tip
  Se você precisar visualizar as informações de `trace_log`, experimente [flamegraph](/pt-BR/interfaces/third-party/gui#clickhouse-flamegraph) e [speedscope](https://www.speedscope.app).
  :::
</VerticalStepper>

<div id="flamegraph">
  ## Gerando flame graphs com a função `flameGraph`
</div>

O ClickHouse fornece a [função de agregação `flameGraph`](/pt-BR/sql-reference/aggregate-functions/reference/flame_graph), que gera um flame graph diretamente a partir de stack traces armazenados em `trace_log`.
A saída é um array de strings em um formato compatível com [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**Sintaxe:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**Argumentos:**

* `traces` — um stacktrace. [`Array(UInt64)`](/pt-BR/sql-reference/data-types/array).
* `size` — um tamanho de alocação para profiling de memória. [`Int64`](/pt-BR/sql-reference/data-types/int-uint).
* `ptr` — um endereço de alocação. [`UInt64`](/pt-BR/sql-reference/data-types/int-uint).

Quando `ptr` não é zero, `flameGraph` correlaciona alocações (`size > 0`) e desalocações (`size < 0`) com o mesmo tamanho e ponteiro.
Apenas as alocações que não foram liberadas são mostradas.
Desalocações sem correspondência são ignoradas.

<div id="cpu-flame-graph">
  ### Flame graph de CPU
</div>

:::note
As consultas abaixo exigem que você tenha o [flamegraph.pl](https://github.com/brendangregg/FlameGraph) instalado.

Você pode fazer isso executando:

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

Substitua `flamegraph.pl` nas consultas abaixo pelo caminho em que o `flamegraph.pl` está localizado na sua máquina
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

Execute sua consulta e, em seguida, gere o flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### Memory flame graph — todas as alocações
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

Execute a consulta e, em seguida, gere o flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### Memory flame graph — alocações não liberadas
</div>

Esta variante correlaciona alocações e desalocações por ponteiro e mostra apenas a memória que não foi liberada durante a consulta.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

Execute a consulta a seguir para gerar o flame graph:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### Memory flame graph — alocações ativas em um dado momento
</div>

Essa abordagem permite encontrar o pico de uso de memória e visualizar o que foi alocado naquele momento.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### Visualizar o uso de memória ao longo do tempo
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### Encontre o instante com o maior uso de memória
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### Crie um flame graph das alocações ativas nesse momento
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### Crie um flame graph das desalocações após esse ponto no tempo (para entender o que foi liberado mais tarde)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## Exemplo
</div>

O trecho de código abaixo:

* Filtra os dados de `trace_log` por um identificador de consulta e pela data atual.
* Agrega por stack trace.
* Usa funções de introspecção para obter um relatório de:
  * Os nomes dos símbolos e as funções correspondentes no código-fonte.
  * As localizações dessas funções no código-fonte.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```