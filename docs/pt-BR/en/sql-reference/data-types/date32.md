---
description: 'Documentação para o tipo de dado Date32 no ClickHouse, que armazena datas
  com um intervalo maior em comparação com Date'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'reference'
---

Uma data. Suporta o mesmo intervalo de datas que [DateTime64](../../sql-reference/data-types/datetime64.md). É armazenado como um inteiro com sinal de 32 bits na ordem de bytes nativa, e o valor representa os dias desde `1900-01-01`. **Importante!** 0 representa `1970-01-01`, e valores negativos representam os dias anteriores a `1970-01-01`.

**Exemplos**

Criando uma tabela com uma coluna do tipo `Date32` e inserindo dados nela:

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**Veja também**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/pt-BR/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/pt-BR/sql-reference/functions/type-conversion-functions#toDate32OrNull)