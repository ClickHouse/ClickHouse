---
description: 'Página com detalhes sobre perfilamento de alocação no ClickHouse'
sidebar_label: 'perfilamento de alocação'
slug: /operations/allocation-profiling
title: 'perfilamento de alocação'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # Perfilamento de alocação
</div>

O ClickHouse usa [jemalloc](https://github.com/jemalloc/jemalloc) como alocador global. O jemalloc inclui ferramentas para amostragem e perfilamento de alocação.

O ClickHouse e o Keeper permitem controlar a amostragem usando configurações, configurações da consulta, comandos `SYSTEM` e comandos four letter word (4LW) no Keeper. Há várias maneiras de inspecionar os resultados:

* Colete amostras em `system.trace_log` com o tipo `JemallocSample` para análise por consulta.
* Visualize estatísticas de memória em tempo real e obtenha perfis de heap pela [interface web do jemalloc](#jemalloc-web-ui) integrada (26.2+).
* Consulte o perfil de heap atual diretamente via SQL usando [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) (26.2+).
* Grave perfis de heap em disco e analise-os com [`jeprof`](#analyzing-heap-profile-files-with-jeprof).

:::note

Este guia se aplica às versões 25.9+.
Para versões anteriores, consulte [perfilamento de alocação para versões anteriores à 25.9](/pt-BR/operations/allocation-profiling-old.md).

:::

<div id="sampling-allocations">
  ## Amostragem de alocações
</div>

Para fazer a amostragem e o perfilamento de alocações, inicie o ClickHouse/Keeper com a config `jemalloc_enable_global_profiler` habilitada:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` fará a amostragem das alocações e armazenará as informações internamente.

Você também pode ativar a amostragem por consulta usando a configuração `jemalloc_enable_profiler`.

:::warning Aviso
Como o ClickHouse é um aplicativo que faz muitas alocações, a amostragem do jemalloc pode causar sobrecarga de desempenho.
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## Armazenando amostras do jemalloc em `system.trace_log`
</div>

Você pode armazenar amostras do jemalloc em `system.trace_log` no tipo `JemallocSample`.
Para habilitar isso globalmente, use a configuração `jemalloc_collect_global_profile_samples_in_trace_log`:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning Aviso
Como o ClickHouse é uma aplicação com uso intensivo de alocação, coletar todas as amostras em system.trace&#95;log pode gerar alta carga no sistema.
:::

Você também pode habilitar isso por consulta usando a configuração `jemalloc_collect_profile_samples_in_trace_log`.

<div id="example-analyzing-memory-usage-trace-log">
  ### Exemplo: analisando o uso de memória de uma consulta
</div>

Primeiro, execute uma consulta com o profiler do jemalloc habilitado e colete as amostras na `system.trace_log`:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
Se o ClickHouse foi iniciado com `jemalloc_enable_global_profiler`, você não precisa habilitar `jemalloc_enable_profiler`.
O mesmo vale para `jemalloc_collect_global_profile_samples_in_trace_log` e `jemalloc_collect_profile_samples_in_trace_log`.
:::

Faça flush do `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```

Em seguida, faça uma consulta para obter o uso acumulado de memória ao longo do tempo:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

Encontre o momento em que o uso de memória foi maior:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

Com esse resultado, veja quais pilhas de alocação estavam mais ativas no momento de pico:

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## Interface web do jemalloc
</div>

:::note
Esta seção se aplica às versões 26.2+.
:::

O ClickHouse oferece uma interface web integrada para visualizar estatísticas de memória do jemalloc no endpoint HTTP `/jemalloc`.
Ela exibe métricas de memória em tempo real com gráficos, incluindo memória allocated, active, resident e mapped, além de estatísticas por arena e por bin.
Você também pode buscar perfis de heap globais e por consulta diretamente pela interface.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    A interface do servidor inclui todas as abas: Summary, Allocations, Arenas, Operations, Global Profiler, Query Profiler e Raw Output.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    A interface do Keeper está disponível na porta de controle HTTP. Essa porta é **desabilitada por padrão** e deve ser habilitada explicitamente definindo `keeper_server.http_control.port` na configuração do Keeper:

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    Depois de habilitada, a interface oferece as mesmas visualizações que o servidor — Summary, Allocations, Arenas, Operations, Global Profiler e Raw Output — exceto pela aba Query Profiler, que requer SQL e `system.trace_log`.

    :::warning Segurança
    A porta de controle HTTP do Keeper não tem autenticação em nível de aplicação. Diferentemente da interface do jemalloc do servidor ClickHouse — em que todas as consultas de dados passam pelo handler HTTP de SQL e exigem credenciais de usuário/senha — os endpoints da API REST do Keeper não exigem autenticação. Isso é consistente com outros endpoints de controle HTTP do Keeper (commands, storage, dashboard).

    Restrinja o acesso a essa porta usando controles de rede: vincule o Keeper ao localhost, use regras de firewall ou coloque-o atrás de um proxy reverso com autenticação. Quando nenhum `listen_host` é configurado, o Keeper escuta apenas em localhost por padrão.
    :::

    O Keeper também expõe endpoints da API REST para acesso programático:

    * `GET /jemalloc/stats` — saída bruta de `malloc_stats_print`
    * `GET /jemalloc/status` — estado do profiling como JSON (`prof_enabled`, `prof_active`, `thread_active_init`, `lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — faz flush de um perfil de heap com simbolização no servidor e retorna collapsed stacks adequadas para renderização de flame graph (padrão) ou o dump bruto do jemalloc
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## Obtendo perfis de heap via SQL
</div>

:::note
Esta seção se aplica às versões 26.2+.
:::

A tabela de sistema `system.jemalloc_profile_text` permite obter e visualizar diretamente via SQL o perfil de heap atual do jemalloc, sem precisar de ferramentas externas nem gravá-lo em disco antes.

A tabela tem uma única coluna:

| Coluna | Tipo   | Descrição                                        |
| ------ | ------ | ------------------------------------------------ |
| `line` | String | Linha do perfil de heap simbolizado do jemalloc. |

Você pode consultar a tabela diretamente — não é necessário gravar um perfil de heap antes:

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### Formato de saída
</div>

O formato de saída é controlado pela configuração `jemalloc_profile_text_output_format`, que oferece suporte a três valores:

* `raw` — heap profile bruto, gerado pelo jemalloc.
* `symbolized` — formato compatível com jeprof, com símbolos de função embutidos. Como os símbolos já estão embutidos, o `jeprof` pode analisar a saída sem precisar do binário do ClickHouse.
* `collapsed` (padrão) — pilhas colapsadas compatíveis com FlameGraph, uma pilha por linha com a contagem de bytes.

Por exemplo, para obter o perfil bruto:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

Para obter a saída simbolizada:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### Configurações adicionais
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, padrão: `true`) — Indica se devem ser incluídos frames inline durante a simbolização. Desabilitar isso acelera significativamente a simbolização, mas reduz a precisão, pois chamadas de função inlined não aparecerão nas pilhas. Afeta apenas os formatos `simbolizado` e `collapsed`.
* `jemalloc_profile_text_collapsed_use_count` (Bool, padrão: `false`) — Ao usar o formato `collapsed`, faz a agregação pela contagem de alocações em vez de bytes.

<div id="example-flamegraph-from-sql">
  ### Exemplo: gerando um flame graph a partir de SQL
</div>

Como o formato de saída padrão é `collapsed`, você pode redirecionar a saída diretamente para o FlameGraph:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Para gerar um flame graph pela contagem de alocações em vez de bytes:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## Gravando perfis de heap em disco
</div>

Se você precisar salvar perfis de heap como arquivos para análise offline com `jeprof`, poderá gravá-los em disco.

Por padrão, o arquivo de perfil de heap será gerado em `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, em que `_pid_` é o PID do ClickHouse e `_seqnum_` é o número de sequência global do perfil de heap atual.
Para o Keeper, o arquivo padrão é `/tmp/jemalloc_keeper._pid_._seqnum_.heap` e segue as mesmas regras.

Para gravar o perfil atual:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    Ele retornará o local do perfil gravado.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

É possível definir outro local adicionando a opção `prof_prefix` à variável de ambiente `MALLOC_CONF`.
Por exemplo, se você quiser gerar perfis na pasta `/data`, em que o prefixo do nome do arquivo será `my_current_profile`, poderá executar o ClickHouse/Keeper com a seguinte variável de ambiente:

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

Ao arquivo gerado serão acrescentados o prefixo PID e o número de sequência.

<div id="analyzing-heap-profile-files-with-jeprof">
  ## Analisando arquivos de perfil de heap com `jeprof`
</div>

Depois de gravar os perfis de heap em disco, eles podem ser analisados com a ferramenta do `jemalloc` chamada [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Ela pode ser instalada de várias formas:

* Usando o gerenciador de pacotes do sistema
* Clonando o [repositório do jemalloc](https://github.com/jemalloc/jemalloc) e executando `autogen.sh` a partir da pasta raiz. Isso disponibilizará o script `jeprof` na pasta `bin`

Há vários formatos de saída disponíveis. Execute `jeprof --help` para ver a lista completa de opções.

<div id="symbolized-heap-profiles">
  ### Perfis de heap simbolizados
</div>

A partir da versão 26.1+, o ClickHouse gera automaticamente perfis de heap simbolizados quando você executa `SYSTEM JEMALLOC FLUSH PROFILE`.
O perfil simbolizado (com a extensão `.symbolized`) contém símbolos de função embutidos e pode ser analisado pelo `jeprof` sem precisar do binário do ClickHouse.

Por exemplo, quando você executa:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

O ClickHouse retornará o caminho para o perfil simbolizado (por exemplo, `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`).

Em seguida, você pode analisá-lo diretamente com `jeprof`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**Nenhum binário necessário**: Ao usar perfis simbolizados (arquivos `.symbolized`), você não precisa informar ao `jeprof` o caminho do binário do ClickHouse. Isso facilita muito a análise de perfis em máquinas diferentes ou depois que o binário é atualizado.

:::

Se você tiver um perfil de heap antigo não simbolizado e ainda tiver acesso ao binário do ClickHouse, poderá usar a abordagem tradicional:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

Para perfis não simbolizados, o `jeprof` usa o `addr2line` para gerar stacktraces, o que pode ser bastante lento.
Se for esse o caso, recomenda-se instalar uma [implementação alternativa](https://github.com/gimli-rs/addr2line) da ferramenta.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

Como alternativa, `llvm-addr2line` também funciona bem (mas observe que `llvm-objdump` não é compatível com `jeprof`)

Depois, use-o assim: `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

Ao comparar dois perfis, você pode usar o argumento `--base`:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### Exemplos
</div>

Usando perfis simbolizados (recomendado):

* Gere um arquivo de texto com cada procedimento em uma linha:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* Gere um arquivo em PDF com um grafo de chamadas:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

Usando perfis não simbolizados (requer binário):

* Gere um arquivo de texto com cada procedimento em uma linha:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* Gere um arquivo PDF com um grafo de chamadas:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Gerando um flame graph
</div>

`jeprof` permite gerar stacks colapsadas para montar flame graphs.

Você precisa usar o argumento `--collapsed`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

Ou com um perfil não simbolizado:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

Depois disso, você pode usar diversas ferramentas para visualizar stacks colapsadas.

A mais popular é o [FlameGraph](https://github.com/brendangregg/FlameGraph), que inclui um script chamado `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Outra ferramenta interessante é o [speedscope](https://www.speedscope.app/), que permite analisar as stacks coletadas de forma mais interativa.

<div id="additional-options-for-profiler">
  ## Opções adicionais para o profiler
</div>

O `jemalloc` oferece várias opções relacionadas ao profiler. Elas podem ser controladas modificando a variável de ambiente `MALLOC_CONF`.
Por exemplo, o intervalo entre as amostras de alocação pode ser controlado com `lg_prof_sample`.
Se você quiser gerar um perfil de heap a cada N bytes, poderá habilitar isso com `lg_prof_interval`.

Recomenda-se consultar a [página de referência](https://jemalloc.net/jemalloc.3.html) do `jemalloc` para ver a lista completa de opções.

<div id="other-resources">
  ## Outros recursos
</div>

ClickHouse/Keeper expõem métricas relacionadas ao `jemalloc` de várias formas diferentes.

:::warning Aviso
É importante ter em mente que nenhuma dessas métricas é sincronizada com as outras, e os valores podem divergir.
:::

<div id="system-table-asynchronous_metrics">
  ### Tabela do sistema `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Referência](/pt-BR/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### Tabela de sistema `jemalloc_bins`
</div>

Contém informações sobre alocações de memória feitas por meio do alocador jemalloc em diferentes classes de tamanho (bins), agregadas de todas as arenas.

[Referência](/pt-BR/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### Tabela de sistema `jemalloc_stats` (26.2+)
</div>

Retorna a saída completa de `malloc_stats_print()` em uma única string. Equivalente ao comando `SYSTEM JEMALLOC STATS`.

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

Todas as métricas relacionadas ao `jemalloc` de `asynchronous_metrics` também são expostas por meio do endpoint Prometheus, tanto no ClickHouse quanto no Keeper.

[Referência](/pt-BR/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### comando 4LW `jmst` no Keeper
</div>

O Keeper oferece suporte ao comando 4LW `jmst`, que retorna [estatísticas básicas do alocador](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```