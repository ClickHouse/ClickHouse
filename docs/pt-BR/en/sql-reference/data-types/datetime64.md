---
description: 'Documentação do tipo de dado DateTime64 no ClickHouse, que armazena
  timestamps com precisão de subsegundos'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

Permite armazenar um instante no tempo que pode ser expresso como uma data do calendário e uma hora do dia, com precisão de subsegundos definida

Tamanho do tick (precisão): 10<sup>-precision</sup> segundos. Intervalo válido: [ 0 : 9 ].
Normalmente, usam-se 3 (milissegundos), 6 (microssegundos) e 9 (nanossegundos).

Valor padrão: 3 (milissegundos).

**Sintaxe:**

```sql
DateTime64(precision, [timezone])
```

Internamente, armazena os dados como um número de &#39;ticks&#39; desde o início da epoch (1970-01-01 00:00:00 UTC), como Int64. A resolução do tick é determinada pelo parâmetro de precisão. Além disso, o tipo `DateTime64` pode armazenar um fuso horário que é o mesmo para toda a coluna, o que afeta como os valores do tipo `DateTime64` são exibidos em formato de texto e como os valores especificados como strings são convertidos (&#39;2020-01-01 05:00:01.000&#39;). O fuso horário não é armazenado nas linhas da tabela (ou no resultset), mas nos metadados da coluna. Veja os detalhes em [DateTime](../../sql-reference/data-types/datetime.md).

Intervalo de valores suportado: [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

O número de dígitos após o separador decimal depende do parâmetro de precisão.

Nota: a precisão do valor máximo é 8. Se a precisão máxima de 9 dígitos (nanossegundos) for usada, o valor máximo suportado será `2262-04-11 23:47:16` em UTC.

<div id="examples">
  ## Exemplos
</div>

1. Criando uma tabela com uma coluna do tipo `DateTime64` e inserindo dados nela:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* Ao inserir um datetime como inteiro, ele é tratado como um Unix timestamp (UTC) com a escala apropriada. `1546300800000` (com precisão 3) representa `'2019-01-01 00:00:00'` UTC. No entanto, como a coluna `timestamp` tem o fuso horário `Asia/Istanbul` (UTC+3) especificado, ao ser exibido como string, o valor será mostrado como `'2019-01-01 03:00:00'`. Ao inserir um datetime como decimal, ele será tratado de forma semelhante a um inteiro, com a diferença de que o valor antes do separador decimal é o Unix timestamp até os segundos, inclusive, e o valor após o separador decimal será tratado como a precisão.
* Ao inserir um valor string como datetime, ele é tratado como estando no fuso horário da coluna. `'2019-01-01 00:00:00'` será tratado como estando no fuso horário `Asia/Istanbul` e armazenado como `1546290000000`.

2. Filtragem de valores `DateTime64`

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

Diferentemente de `DateTime`, os valores `DateTime64` não são convertidos automaticamente a partir de `String`.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

Ao contrário do processo de inserção, a função `toDateTime64` tratará todos os valores como a variante decimal, portanto a precisão deve
ser fornecida após o separador decimal.

3. Obtendo o fuso horário de um valor do tipo `DateTime64`:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Conversão de fuso horário

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**Veja também**

* [Funções de conversão de tipos](../../sql-reference/functions/type-conversion-functions.md)
* [Funções para trabalhar com datas e horas](../../sql-reference/functions/date-time-functions.md)
* [A configuração `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [A configuração `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [O parâmetro de configuração do servidor `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [A configuração `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Operadores para trabalhar com datas e horas](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Tipo de dado `Date`](../../sql-reference/data-types/date.md)
* [Tipo de dado `DateTime`](../../sql-reference/data-types/datetime.md)