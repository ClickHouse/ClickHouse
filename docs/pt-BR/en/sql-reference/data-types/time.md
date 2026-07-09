---
description: 'Documentação do tipo de dado Time no ClickHouse, que armazena o
  horário com precisão de segundos'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

O tipo de dado `Time` representa um horário com componentes de hora, minuto e segundo.
Ele é independente de qualquer data do calendário e é adequado para valores que não precisam de componentes de dia, mês e ano.

Sintaxe:

```sql
Time
```

Intervalo da representação textual: [-999:59:59, 999:59:59].

Resolução: 1 segundo.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

**Representação e desempenho**.
O tipo de dado `Time` armazena internamente um inteiro com sinal de 32 bits que codifica os segundos.
Valores dos tipos `Time` e `DateTime` têm o mesmo tamanho em bytes e, portanto, desempenho comparável.

**Normalização**.
Ao converter strings para `Time`, os componentes de tempo são normalizados, e não validados.
Por exemplo, `25:70:70` é interpretado como `26:11:10`.

**Valores negativos**.
Sinais de menos no início são aceitos e preservados.
Valores negativos normalmente surgem de operações aritméticas sobre valores `Time`.
Para o tipo `Time`, entradas negativas são preservadas tanto em entradas de texto (por exemplo, `'-01:02:03'`) quanto em entradas numéricas (por exemplo, `-3723`).

**Saturação**.
O componente de hora do dia é limitado ao intervalo [-999:59:59, 999:59:59].
Valores com horas acima de 999 (ou abaixo de -999) são representados e convertidos de volta via texto como `999:59:59` (ou `-999:59:59`).

**Fusos horários**.
`Time` não oferece suporte a fusos horários, ou seja, valores `Time` são interpretados sem contexto regional.
Especificar um fuso horário para `Time` como parâmetro de tipo ou durante a criação de um valor resulta em erro.
Da mesma forma, tentativas de aplicar ou alterar o fuso horário em colunas `Time` não são suportadas e resultam em erro.
Valores `Time` não são reinterpretados silenciosamente em fusos horários diferentes.

<div id="examples">
  ## Exemplos
</div>

**1.** Criação de uma tabela com uma coluna do tipo `Time` e inserção de dados nela:

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** Filtragem por valores de `Time`

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

Os valores da coluna `Time` podem ser filtrados usando um valor de string no predicado `WHERE`. Ele será convertido automaticamente para `Time`:

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** Verificando o tipo resultante:

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## Adição com Date
</div>

Um valor de [Time](time.md) pode ser adicionado a um valor de [Date](date.md) ou [Date32](date32.md), resultando em um [DateTime](datetime.md) ou [DateTime64](datetime64.md):

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

Consulte [Adição de data e hora](../operators/index.md#date-time-addition) para obter detalhes sobre todas as combinações compatíveis e os tipos de resultado.

<div id="see-also">
  ## Veja também
</div>

* [Funções de conversão de tipo](../functions/type-conversion-functions.md)
* [Funções para trabalhar com datas e horas](../functions/date-time-functions.md)
* [Funções para trabalhar com arrays](../functions/array-functions.md)
* [A configuração `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [A configuração `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [O parâmetro `timezone` da configuração do servidor](../../operations/server-configuration-parameters/settings.md#timezone)
* [A configuração `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [O tipo de dado `DateTime`](datetime.md)
* [O tipo de dado `Date`](date.md)