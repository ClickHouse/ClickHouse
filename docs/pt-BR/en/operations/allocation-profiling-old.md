---
description: 'Página com detalhes sobre o profiling de alocação no ClickHouse'
sidebar_label: 'Profiling de alocação para versões anteriores à 25.9'
slug: /operations/allocation-profiling-old
title: 'Profiling de alocação para versões anteriores à 25.9'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # Profiling de alocação para versões anteriores à 25.9
</div>

O ClickHouse usa o [jemalloc](https://github.com/jemalloc/jemalloc) como alocador global. O jemalloc vem com algumas ferramentas para amostragem e profiling de alocação.
Para tornar o profiling de alocação mais prático, são fornecidos comandos `SYSTEM` junto com comandos de quatro letras (4LW) no Keeper.

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## Amostragem de alocações e gravação de heap profiles
</div>

Se você quiser fazer amostragem e profiling de alocações no `jemalloc`, precisará iniciar o ClickHouse/Keeper com o profiling habilitado usando a variável de ambiente `MALLOC_CONF`:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` fará a amostragem das alocações e armazenará as informações internamente.

Você pode instruir o `jemalloc` a gravar o perfil atual executando:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Por padrão, o arquivo de heap profile será gerado em `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, em que `_pid_` é o PID do ClickHouse e `_seqnum_` é o número de sequência global do heap profile atual.
Para o Keeper, o arquivo padrão é `/tmp/jemalloc_keeper._pid_._seqnum_.heap` e segue as mesmas regras.

É possível definir outro local acrescentando a opção `prof_prefix` à variável de ambiente `MALLOC_CONF`.
Por exemplo, se você quiser gerar perfis na pasta `/data`, em que o prefixo do nome do arquivo será `my_current_profile`, poderá executar o ClickHouse/Keeper com a seguinte variável de ambiente:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

O arquivo gerado terá o prefixo PID e o número de sequência anexados.

<div id="analyzing-heap-profiles">
  ## Analisando perfis de heap
</div>

Depois que os perfis de heap forem gerados, é preciso analisá-los.
Para isso, pode-se usar a ferramenta do `jemalloc` chamada [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Ela pode ser instalada de várias formas:

* Usando o gerenciador de pacotes do sistema
* Clonando o [repositório do jemalloc](https://github.com/jemalloc/jemalloc) e executando `autogen.sh` na pasta raiz. Isso disponibilizará o script `jeprof` na pasta `bin`

:::note
O `jeprof` usa `addr2line` para gerar stacktraces, o que pode ser bem lento.
Se esse for o caso, recomenda-se instalar uma [implementação alternativa](https://github.com/gimli-rs/addr2line) da ferramenta.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

Há muitos formatos diferentes que podem ser gerados a partir do heap profile com o `jeprof`.
Recomenda-se executar `jeprof --help` para obter informações sobre o uso e as diversas opções que a ferramenta oferece.

Em geral, o comando `jeprof` é usado da seguinte forma:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

Se você quiser comparar quais alocações ocorreram entre dois perfis, pode definir o argumento `base`:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### Exemplos
</div>

* se você quiser gerar um arquivo de texto com cada procedimento escrito em uma linha:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* se você quiser gerar um arquivo PDF com um grafo de chamadas:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Gerando um flame graph
</div>

`jeprof` permite gerar collapsed stacks para criar flame graphs.

Você precisa usar o argumento `--collapsed`:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

Depois disso, você pode usar várias ferramentas para visualizar stacks colapsadas.

A mais popular é o [FlameGraph](https://github.com/brendangregg/FlameGraph), que inclui um script chamado `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Outra ferramenta interessante é o [speedscope](https://www.speedscope.app/), que permite analisar as stacks coletadas de forma mais interativa.

<div id="controlling-allocation-profiler-during-runtime">
  ## Controlando o profiler de alocação em tempo de execução
</div>

Se o ClickHouse/Keeper for iniciado com o profiler habilitado, comandos adicionais para desabilitar/habilitar o profiling de alocação em tempo de execução serão aceitos.
Com esses comandos, fica mais fácil fazer o profiling apenas em intervalos específicos.

Para desabilitar o profiler:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Para habilitar o profiler:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Também é possível controlar o estado inicial do profiler definindo a opção `prof_active`, que é habilitada por padrão.
Por exemplo, se você não quiser amostrar alocações durante a inicialização, mas apenas depois dela, poderá habilitar o profiler mais tarde. Você pode iniciar o ClickHouse/Keeper com a seguinte variável de ambiente:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

O profiler pode ser ativado depois.

<div id="additional-options-for-profiler">
  ## Opções adicionais para o profiler
</div>

O `jemalloc` oferece várias opções relacionadas ao profiler. Elas podem ser controladas modificando a variável de ambiente `MALLOC_CONF`.
Por exemplo, o intervalo entre as amostras de alocação pode ser controlado com `lg_prof_sample`.
Se quiser gerar o dump do heap profile a cada N bytes, você pode habilitá-lo com `lg_prof_interval`.

Recomenda-se consultar a [página de referência](https://jemalloc.net/jemalloc.3.html) do `jemalloc` para ver a lista completa de opções.

<div id="other-resources">
  ## Outros recursos
</div>

ClickHouse/Keeper expõem métricas relacionadas ao `jemalloc` de várias formas diferentes.

:::warning Aviso
É importante ter em mente que nenhuma dessas métricas é sincronizada com as demais, e os valores podem variar.
:::

<div id="system-table-asynchronous_metrics">
  ### tabela do sistema `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Referência](/pt-BR/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### Tabela do sistema `jemalloc_bins`
</div>

Contém informações sobre alocações de memória feitas pelo alocador jemalloc em diferentes classes de tamanho (bins), agregadas de todas as arenas.

[Referência](/pt-BR/operations/system-tables/jemalloc_bins)

<div id="prometheus">
  ### Prometheus
</div>

Todas as métricas relacionadas ao `jemalloc` de `asynchronous_metrics` também são expostas por meio do endpoint Prometheus tanto no ClickHouse quanto no Keeper.

[Referência](/pt-BR/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Comando 4LW `jmst` no Keeper
</div>

O Keeper oferece suporte ao comando 4LW `jmst`, que retorna [estatísticas básicas do alocador de memória](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```