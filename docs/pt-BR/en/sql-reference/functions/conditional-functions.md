---
description: 'Documentação sobre Funções Condicionais'
sidebar_label: 'Condicional'
slug: /sql-reference/functions/conditional-functions
title: 'Funções Condicionais'
doc_type: 'reference'
---

<div id="overview">
  ## Visão geral
</div>

<div id="using-conditional-results-directly">
  ### Usando resultados condicionais diretamente
</div>

As condicionais sempre resultam em `0`, `1` ou `NULL`. Portanto, você pode usar esses resultados diretamente assim:

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

<div id="null-values-in-conditionals">
  ### Valores NULL em condicionais
</div>

Quando valores `NULL` estão envolvidos em condicionais, o resultado também será `NULL`.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Portanto, você deve construir suas consultas com cuidado se os tipos forem `Nullable`.

O exemplo a seguir demonstra isso ao não adicionar a condição `equals` ao `multiIf`.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

<div id="case-statement">
  ### Instrução CASE
</div>

A expressão CASE no ClickHouse fornece lógica condicional semelhante ao operador CASE do SQL. Ela avalia as condições e retorna valores com base na primeira condição correspondente.

O ClickHouse oferece suporte a duas formas de CASE:

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   Essa forma permite total flexibilidade e é implementada internamente com a função [multiIf](/pt-BR/sql-reference/functions/conditional-functions#multiIf). Cada condição é avaliada de forma independente, e as expressões podem incluir valores não constantes.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   Essa forma mais compacta é otimizada para a correspondência de valores constantes e usa internamente `caseWithExpression()`.

Por exemplo, o código a seguir é válido:

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

Essa forma também não exige que as expressões de retorno sejam constantes.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### Ressalvas
</div>

O ClickHouse determina o tipo de resultado de uma expressão CASE (ou seu equivalente interno, como `multiIf`) antes de avaliar qualquer condição. Isso é importante quando as expressões de retorno têm tipos diferentes, como fusos horários distintos ou tipos numéricos diferentes.

* O tipo de resultado é selecionado com base no tipo compatível mais amplo entre todos os ramos.
* Depois que esse tipo é selecionado, todos os outros ramos são convertidos implicitamente para ele, mesmo que sua lógica nunca seja executada em tempo de execução.
* Para tipos como DateTime64, em que o fuso horário faz parte da assinatura do tipo, isso pode levar a um comportamento inesperado: o primeiro fuso horário encontrado pode ser usado em todos os ramos, mesmo quando outros ramos especificam fusos horários diferentes.

Por exemplo, abaixo todas as linhas retornam o timestamp no fuso horário do primeiro ramo correspondente, ou seja, `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

Aqui, o ClickHouse identifica vários tipos de retorno `DateTime64(3, <timezone>)`. Ele infere o tipo comum como `DateTime64(3, 'Asia/Kolkata'` por ser o primeiro que encontra, convertendo implicitamente os outros ramos para esse tipo.

Isso pode ser contornado convertendo para string, para preservar a formatação de fuso horário desejada:

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  O conteúdo interno das tags abaixo é substituído durante a compilação do framework de documentação por 
  documentação gerada a partir de system.functions. Não modifique nem remova as tags.
  Veja: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }