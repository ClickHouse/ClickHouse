---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: 'Retorna uma tabela com uma única coluna `number` que contém uma sequência de inteiros.'
doc_type: 'reference'
---

* `numbers()` – Retorna uma tabela infinita com uma única coluna `number` (UInt64) que contém inteiros em ordem crescente, começando em 0. Use `LIMIT` (e, opcionalmente, `OFFSET`) para limitar o número de linhas.

* `numbers(N)` – Retorna uma tabela com uma única coluna `number` (UInt64) que contém inteiros de 0 a `N - 1`.

* `numbers(N, M)` – Retorna uma tabela com uma única coluna `number` (UInt64) que contém `M` inteiros de `N` a `N + M - 1`.

* `numbers(N, M, S)` – Retorna uma tabela com uma única coluna `number` (UInt64) que contém valores em `[N, N + M)` com passo `S` (cerca de `M / S` linhas, arredondadas para cima). `S` deve ser `>= 1`.

Isso é semelhante à system table [`system.numbers`](/pt-BR/operations/system-tables/numbers). Ela pode ser usada para testes e para gerar valores sequenciais.

As consultas a seguir são equivalentes:

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

As consultas a seguir também são equivalentes:

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

As consultas a seguir também são equivalentes:

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

<div id="examples">
  ### Exemplos
</div>

Os 10 primeiros números.

```sql
SELECT * FROM numbers(10);
```

```response
 ┌─number─┐
 │      0 │
 │      1 │
 │      2 │
 │      3 │
 │      4 │
 │      5 │
 │      6 │
 │      7 │
 │      8 │
 │      9 │
 └────────┘
```

Gere uma sequência de datas de 2010-01-01 a 2010-12-31.

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

Encontre o primeiro `UInt64` `>= 10^15` cujo `sipHash64(number)` tenha 20 bits zero no final.

```sql
SELECT number
FROM numbers()
WHERE number >= 1e15
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

<div id="notes">
  ### Observações
</div>

* Por motivos de desempenho, se você souber quantas linhas precisa, prefira versões com limite (`numbers(N)`, `numbers(N, M[, S])`) em vez de `numbers()` / `system.numbers` sem limite.
* Para geração paralela, use `numbers_mt(...)` ou a tabela [`system.numbers_mt`](/pt-BR/operations/system-tables/numbers_mt). Observe que os resultados podem ser retornados em qualquer ordem.