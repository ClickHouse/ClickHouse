---
description: 'Documentação para o tipo de dado DateTime no ClickHouse, que armazena
  timestamps com precisão de segundos'
sidebar_label: 'DateTime'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: 'DateTime'
doc_type: 'reference'
---

Permite armazenar um instante no tempo, que pode ser expresso como uma data de calendário e uma hora do dia.

Sintaxe:

```sql
DateTime([timezone])
```

Faixa de valores suportada: [1970-01-01 00:00:00, 2106-02-07 06:28:15].

Resolução: 1 segundo.

<div id="speed">
  ## Velocidade
</div>

O tipo de dado `Date` é mais rápido que `DateTime` na *maioria* dos casos.

O tipo `Date` requer 2 bytes de armazenamento, enquanto `DateTime` requer 4. No entanto, durante a compressão, a diferença de tamanho entre `Date` e `DateTime` se torna mais significativa. Isso acontece porque os minutos e segundos em `DateTime` são menos compressíveis. Filtrar e agregar `Date` em vez de `DateTime` também é mais rápido.

<div id="usage-remarks">
  ## Observações de uso
</div>

O instante é salvo como um [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), independentemente do fuso horário ou do horário de verão. O fuso horário afeta como os valores do tipo `DateTime` são exibidos em formato de texto e como os valores especificados como strings são convertidos (`'2020-01-01 05:00:01'`).

Um Unix timestamp independente de fuso horário é armazenado nas tabelas, e o fuso horário é usado para convertê-lo para formato de texto ou de volta durante a importação/exportação de dados, ou para fazer cálculos de calendário com os valores (por exemplo, funções `toDate`, `toHour` etc.). O fuso horário não é armazenado nas linhas da tabela (nem no conjunto de resultados), mas nos metadados da coluna.

Uma lista de fusos horários suportados pode ser encontrada no [IANA Time Zone Database](https://www.iana.org/time-zones) e também pode ser consultada com `SELECT * FROM system.time_zones`. [A lista](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) também está disponível na Wikipedia.

Você pode definir explicitamente um fuso horário para colunas do tipo `DateTime` ao criar uma tabela. Exemplo: `DateTime('UTC')`. Se o fuso horário não for definido, o ClickHouse usa o valor do parâmetro [timezone](../../operations/server-configuration-parameters/settings.md#timezone) nas configurações do servidor ou nas configurações do sistema operacional no momento da inicialização do servidor ClickHouse.

O [clickhouse-client](../../interfaces/client.md) aplica o fuso horário do servidor por padrão se um fuso horário não for explicitamente definido ao inicializar o tipo de dado. Para usar o fuso horário do cliente, execute `clickhouse-client` com o parâmetro `--use_client_time_zone`.

O ClickHouse exibe os valores de acordo com o valor da configuração [date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format). O formato de texto `YYYY-MM-DD hh:mm:ss` é usado por padrão. Além disso, você pode alterar a saída com a função [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime).

Ao inserir dados no ClickHouse, você pode usar diferentes formatos de strings de data e hora, dependendo do valor da configuração [date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format).

<div id="examples">
  ## Exemplos
</div>

**1.** Criando uma tabela com uma coluna do tipo `DateTime` e inserindo dados nela:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* Ao inserir um datetime como inteiro, ele é tratado como um timestamp Unix (UTC). `1546300800` representa `'2019-01-01 00:00:00'` UTC. No entanto, como a coluna `timestamp` tem o fuso horário `Asia/Istanbul` (UTC+3) especificado, ao ser exibido como string o valor será mostrado como `'2019-01-01 03:00:00'`
* Ao inserir um valor de string como datetime, ele é tratado como pertencente ao fuso horário da coluna. `'2019-01-01 00:00:00'` será tratado como estando no fuso horário `Asia/Istanbul` e salvo como `1546290000`.

**2.** Filtragem de valores `DateTime`

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

Os valores da coluna `DateTime` podem ser filtrados usando um valor textual no predicado `WHERE`. Ele será convertido para `DateTime` automaticamente:

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Obtendo o fuso horário de uma coluna do tipo `DateTime`:

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** Conversão de fuso horário

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

Como a conversão de fuso horário altera apenas os metadados, a operação não tem custo de processamento.

<div id="limitations-on-time-zones-support">
  ## Limitações no suporte a fusos horários
</div>

Alguns fusos horários podem não ter suporte completo. Há alguns casos:

Se o deslocamento em relação ao UTC não for múltiplo de 15 minutos, o cálculo de horas e minutos pode ficar incorreto. Por exemplo, o fuso horário de Monróvia, na Libéria, tinha deslocamento UTC -0:44:30 antes de 7 Jan 1972. Se você estiver fazendo cálculos com horários históricos no fuso horário de Monróvia, as funções de processamento de tempo podem gerar resultados incorretos. Mesmo assim, os resultados após 7 Jan 1972 estarão corretos.

Se a transição de horário (devido ao horário de verão ou por outros motivos) tiver sido feita em um instante que não seja múltiplo de 15 minutos, você também poderá obter resultados incorretos nesse dia específico.

Datas de calendário não monotônicas. Por exemplo, em Happy Valley - Goose Bay, o horário foi ajustado uma hora para trás às 00:01:00 de 7 Nov 2010 (um minuto após a meia-noite). Assim, depois que 6 Nov terminou, as pessoas vivenciaram um minuto inteiro de 7 Nov; então o horário voltou para 23:01 de 6 Nov e, após mais 59 minutos, 7 Nov começou novamente. O ClickHouse ainda não oferece suporte a esse tipo de situação curiosa. Durante esses dias, os resultados das funções de processamento de tempo podem ficar ligeiramente incorretos.

Há um problema semelhante na estação antártica Casey no ano de 2010. O horário foi atrasado em três horas em 5 Mar, às 02:00. Se você estiver trabalhando em uma estação antártica, não tenha receio de usar o ClickHouse. Apenas certifique-se de definir o fuso horário como UTC ou esteja ciente das imprecisões.

Mudanças de horário de vários dias. Algumas ilhas do Pacífico mudaram seu deslocamento de UTC+14 para UTC-12. Isso não é um problema, mas podem ocorrer algumas imprecisões se você fizer cálculos com o fuso horário delas para momentos históricos nos dias da mudança.

<div id="handling-daylight-saving-time-dst">
  ## Tratamento do horário de verão (DST)
</div>

O tipo DateTime do ClickHouse com fusos horários pode apresentar comportamento inesperado durante transições de horário de verão (DST), especialmente quando:

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) está definido como `simple`.
* Os relógios são atrasados (&quot;Fall Back&quot;), causando uma sobreposição de uma hora.
* Os relógios são adiantados (&quot;Spring Forward&quot;), causando uma lacuna de uma hora.

Por padrão, o ClickHouse sempre escolhe a ocorrência mais antiga de um horário sobreposto e pode interpretar horários inexistentes durante adiantamentos do relógio.

Por exemplo, considere a seguinte transição do horário de verão (DST) para o horário padrão.

* Em 29 de outubro de 2023, às 02:00:00, os relógios voltam para 01:00:00 (BST → GMT).
* A hora entre 01:00:00 e 01:59:59 aparece duas vezes (uma em BST e outra em GMT)
* O ClickHouse sempre escolhe a primeira ocorrência (BST), causando resultados inesperados ao adicionar intervalos de tempo.

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

Da mesma forma, durante a transição do Horário Padrão para o Horário de Verão, pode parecer que uma hora foi &quot;pulada&quot;.

Por exemplo:

* Em 26 de março de 2023, às `00:59:59`, os relógios são adiantados para 02:00:00 (GMT → BST).
* O intervalo `01:00:00` – `01:59:59` não existe.

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

Neste caso, ClickHouse desloca o horário inexistente `2023-03-26 01:30:00` para trás, para `2023-03-26 00:30:00`.

<div id="see-also">
  ## Veja também
</div>

* [Funções de conversão de tipos](../../sql-reference/functions/type-conversion-functions.md)
* [Funções para trabalhar com datas e horas](../../sql-reference/functions/date-time-functions.md)
* [Funções para trabalhar com arrays](../../sql-reference/functions/array-functions.md)
* [A configuração `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [A configuração `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [O parâmetro de configuração do servidor `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [A configuração `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Operadores para trabalhar com datas e horas](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [O tipo de dado `Date`](../../sql-reference/data-types/date.md)