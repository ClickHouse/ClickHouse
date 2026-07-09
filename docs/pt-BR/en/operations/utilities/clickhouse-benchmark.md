---
description: 'Documentação do clickhouse-benchmark '
sidebar_label: 'clickhouse-benchmark'
sidebar_position: 61
slug: /operations/utilities/clickhouse-benchmark
title: 'clickhouse-benchmark'
doc_type: 'reference'
---

Conecta-se a um servidor ClickHouse e envia repetidamente as consultas informadas.

**Sintaxe**

```bash
$ clickhouse-benchmark --query ["single query"] [keys]
```

ou

```bash
$ echo "single query" | clickhouse-benchmark [keys]
```

ou

```bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Se você quiser enviar um conjunto de consultas, crie um arquivo de texto e coloque cada consulta em uma linha separada nesse arquivo. Por exemplo:

```sql
SELECT * FROM system.numbers LIMIT 10000000;
SELECT 1;
```

Em seguida, passe este arquivo para a entrada padrão do `clickhouse-benchmark`:

```bash
clickhouse-benchmark [keys] < queries_file;
```

<div id="clickhouse-benchmark-command-line-options">
  ## Opções de linha de comando
</div>

* `--query=QUERY` — Consulta a executar. Se este parâmetro não for informado, o `clickhouse-benchmark` lerá as consultas da entrada padrão.
* `--query_id=ID` — ID da consulta.
* `--query_id_prefix=ID_PREFIX` — Prefixo do ID da consulta.
* `--queries-format=FORMAT` — Formato das consultas lidas da entrada padrão. Valores possíveis: `tsv` (padrão, uma consulta com escape de tabulação por linha) e `script` (analisa a entrada como um script com várias consultas separadas por ponto e vírgula). Limitação de `script`: consultas `INSERT ... FORMAT` devem estar em uma única linha.
* `-c N`, `--concurrency=N` — Número de consultas que o `clickhouse-benchmark` envia simultaneamente. Valor padrão: 1.
* `-C N`, `--max_concurrency=N` — Aumenta gradualmente o número de consultas paralelas até o valor especificado, gerando um relatório para cada nível de concorrência.
* `--precise` — Habilita relatórios precisos por intervalo com métricas ponderadas.
* `-d N`, `--delay=N` — Intervalo em segundos entre relatórios intermediários (para desabilitar os relatórios, defina 0). Valor padrão: 1.
* `-h HOST`, `--host=HOST` — Host do servidor. Valor padrão: `localhost`. Para o [modo de comparação](#clickhouse-benchmark-comparison-mode), você pode usar várias opções `-h`.
* `-i N`, `--iterations=N` — Número total de consultas. Valor padrão: 0 (repete indefinidamente).
* `-r`, `--randomize` — Executa as consultas em ordem aleatória se houver mais de uma consulta de entrada.
* `-s`, `--secure` — Usa conexão `TLS`.
* `-t N`, `--timelimit=N` — Limite de tempo em segundos. O `clickhouse-benchmark` para de enviar consultas quando o limite de tempo especificado é atingido. Valor padrão: 0 (limite de tempo desabilitado).
* `--port=N` — Porta do servidor. Valor padrão: 9000. Para o [modo de comparação](#clickhouse-benchmark-comparison-mode), você pode usar várias opções `--port`.
* `--confidence=N` — Nível de confiança para o teste t. Valores possíveis: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99,5%). Valor padrão: 5. No [modo de comparação](#clickhouse-benchmark-comparison-mode), o `clickhouse-benchmark` executa o [teste t de Student independente de duas amostras](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) para determinar se as duas distribuições não diferem com o nível de confiança selecionado.
* `--cumulative` — Imprime dados acumulados em vez de dados por intervalo.
* `--database=DATABASE_NAME` — nome do banco de dados do ClickHouse. Valor padrão: `default`.
* `--user=USERNAME` — Nome de usuário do ClickHouse. Valor padrão: `default`.
* `--password=PSWD` — Senha do usuário do ClickHouse. Valor padrão: string vazia.
* `--stacktrace` — Exibe stack traces. Quando a opção está definida, o `clickhouse-benchmark` exibe stack traces de exceções.
* `--stage=WORD` — Estágio de processamento da consulta no servidor. O ClickHouse interrompe o processamento da consulta e retorna uma resposta ao `clickhouse-benchmark` no estágio especificado. Valores possíveis: `complete`, `fetch_columns`, `with_mergeable_state`. Valor padrão: `complete`.
* `--roundrobin` — Em vez de comparar consultas entre diferentes `--host`/`--port`, escolhe um `--host`/`--port` aleatório para cada consulta e a envia para ele.
* `--reconnect=N` — Controla o comportamento de reconexão. Valores possíveis: 0 (nunca reconecta), 1 (reconecta a cada consulta) ou N (reconecta a cada N consultas). Valor padrão: 0.
* `--max-consecutive-errors=N` — Número de erros consecutivos permitidos. Valor padrão: 0.
* `--ignore-error`,`--continue_on_errors` — Continua o teste mesmo que as consultas falhem.
* `--client-side-time` — Exibe o tempo incluindo a comunicação de rede em vez do tempo do lado do servidor; observe que, para versões do servidor anteriores à 22.8, sempre exibimos o tempo do lado do cliente.
* `--proto-caps` — Habilita/desabilita a fragmentação na transferência de dados. opções (podem ser separadas por vírgula): `chunked_optional`, `notchunked`, `notchunked_optional`, `send_chunked`, `send_chunked_optional`, `send_notchunked`, `send_notchunked_optional`, `recv_chunked`, `recv_chunked_optional`, `recv_notchunked`, `recv_notchunked_optional`. Valor padrão: `notchunked`.
* `--help` — Exibe a mensagem de ajuda.
* `--verbose` — Aumenta a verbosidade da mensagem de ajuda.

Se você quiser aplicar algumas [configurações](/pt-BR/operations/settings/overview) para consultas, passe-as como uma opção `--<session setting name>= SETTING_VALUE`. Por exemplo, `--max_memory_usage=1048576`.

<div id="clickhouse-benchmark-environment-variable-options">
  ## Opções de variáveis de ambiente
</div>

O nome de usuário, a senha e o host podem ser definidos por meio das variáveis de ambiente `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` e `CLICKHOUSE_HOST`.
Os argumentos da linha de comando `--user`, `--password` ou `--host` têm precedência sobre as variáveis de ambiente.

<div id="clickhouse-benchmark-output">
  ## Saída
</div>

Por padrão, `clickhouse-benchmark` emite um relatório a cada intervalo de `--delay`.

Exemplo do relatório:

```text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

No relatório, você pode encontrar:

* Número de consultas no campo `Queries executed:`.

* String de status contendo, na seguinte ordem:

  * Endpoint do servidor ClickHouse.
  * Número de consultas processadas.
  * QPS: Quantas consultas o servidor executou por segundo durante um período especificado no argumento `--delay`.
  * RPS: Quantas linhas o servidor lê por segundo durante um período especificado no argumento `--delay`.
  * MiB/s: Quantos mebibytes o servidor lê por segundo durante um período especificado no argumento `--delay`.
  * result RPS: Quantas linhas o servidor retorna no resultado de uma consulta por segundo durante um período especificado no argumento `--delay`.
  * result MiB/s. Quantos mebibytes o servidor retorna no resultado de uma consulta por segundo durante um período especificado no argumento `--delay`.

* Percentis do tempo de execução das consultas.

<div id="clickhouse-benchmark-comparison-mode">
  ## Modo de comparação
</div>

`clickhouse-benchmark` pode comparar o desempenho de dois servidores ClickHouse em execução.

Para usar o modo de comparação, especifique os endpoints de ambos os servidores com dois pares das opções `--host`, `--port`. As opções são associadas de acordo com a posição na lista de argumentos: o primeiro `--host` é associado ao primeiro `--port` e assim por diante. O `clickhouse-benchmark` estabelece conexões com ambos os servidores e, em seguida, envia consultas. Cada consulta é direcionada a um servidor selecionado aleatoriamente. Os resultados são exibidos em uma tabela.

<div id="clickhouse-benchmark-example">
  ## Exemplo
</div>

```bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark --host=localhost --port=9001 --host=localhost --port=9000 -i 10
```

```text
Loaded 1 queries.

Queries executed: 5.

localhost:9001, queries 2, QPS: 3.764, RPS: 75446929.370, MiB/s: 575.614, result RPS: 37639659.982, result MiB/s: 287.168.
localhost:9000, queries 3, QPS: 3.815, RPS: 76466659.385, MiB/s: 583.394, result RPS: 38148392.297, result MiB/s: 291.049.

0.000%          0.258 sec.      0.250 sec.
10.000%         0.258 sec.      0.250 sec.
20.000%         0.258 sec.      0.250 sec.
30.000%         0.258 sec.      0.267 sec.
40.000%         0.258 sec.      0.267 sec.
50.000%         0.273 sec.      0.267 sec.
60.000%         0.273 sec.      0.267 sec.
70.000%         0.273 sec.      0.267 sec.
80.000%         0.273 sec.      0.269 sec.
90.000%         0.273 sec.      0.269 sec.
95.000%         0.273 sec.      0.269 sec.
99.000%         0.273 sec.      0.269 sec.
99.900%         0.273 sec.      0.269 sec.
99.990%         0.273 sec.      0.269 sec.

No difference proven at 99.5% confidence
```