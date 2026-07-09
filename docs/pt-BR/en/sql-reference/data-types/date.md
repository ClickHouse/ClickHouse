---
description: 'Documentação para o tipo de dado Date no ClickHouse'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

Uma data. Armazenada em dois bytes como o número de dias desde 1970-01-01 (sem sinal). Permite armazenar valores desde pouco depois do início da Unix epoch até o limite superior definido por uma constante na fase de compilação (atualmente, isso vai até o ano 2149, mas o último ano com suporte completo é 2148).

Intervalo de valores suportado: [1970-01-01, 2149-06-06].

O valor da data é armazenado sem o fuso horário.

**Exemplo**

Criando uma tabela com uma coluna do tipo `Date` e inserindo dados nela:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**Veja também**

* [Funções para trabalhar com datas e horas](../../sql-reference/functions/date-time-functions.md)
* [Operadores para trabalhar com datas e horas](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [tipo de dado `DateTime`](../../sql-reference/data-types/datetime.md)