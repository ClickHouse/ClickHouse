---
description: 'Documentação da função arrayJoin'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'função arrayJoin'
doc_type: 'reference'
---

Esta é uma função bastante incomum.

Funções normais não alteram um conjunto de linhas, apenas os valores em cada linha (`map`).
Funções de agregação condensam um conjunto de linhas (fold ou reduce).
A função `arrayJoin` pega cada linha e gera um conjunto de linhas (unfold).

Essa função recebe um array como argumento e propaga a linha de origem em várias linhas, de acordo com o número de elementos no array.
Todos os valores das colunas são simplesmente copiados, exceto os valores da coluna à qual essa função é aplicada; ela é substituída pelo valor correspondente do array.

:::note
Se o array estiver vazio, `arrayJoin` não produz nenhuma linha.
Para retornar uma única linha contendo o valor padrão do tipo array, você pode envolvê-lo com [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle), por exemplo: `arrayJoin(emptyArrayToSingle(...))`.
:::

Por exemplo:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

A função `arrayJoin` afeta todas as partes da consulta, incluindo a cláusula `WHERE`. Observe que o resultado da consulta abaixo é `2`, embora a subconsulta tenha retornado 1 linha.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

Uma consulta pode usar várias funções `arrayJoin`. Nesse caso, a transformação é aplicada várias vezes, e as linhas são multiplicadas.
Por exemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### Boa prática
</div>

Usar vários `arrayJoin` com a mesma expressão pode não produzir os resultados esperados devido à eliminação de subexpressões comuns.
Nesses casos, considere modificar expressões de array repetidas com operações extras que não afetem o resultado do join. Por exemplo, `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

Exemplo:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

Observe a sintaxe de [`ARRAY JOIN`](../statements/select/array-join.md) na consulta SELECT, que oferece mais possibilidades.
`ARRAY JOIN` permite converter vários arrays com o mesmo número de elementos de uma só vez.

Exemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

Ou você pode usar [`Tuple`](../data-types/tuple.md)

Exemplo:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

O nome `arrayJoin` no ClickHouse vem de sua semelhança conceitual com a operação JOIN, mas aplicada a arrays dentro de uma única linha. Enquanto as junções tradicionais combinam linhas de tabelas diferentes, `arrayJoin` &quot;junta&quot; cada elemento de um array em uma linha, produzindo várias linhas — uma para cada elemento do array — enquanto duplica os valores das outras colunas. O ClickHouse também fornece a sintaxe da cláusula [`ARRAY JOIN`](/pt-BR/sql-reference/statements/select/array-join), o que torna essa relação com as operações JOIN tradicionais ainda mais explícita ao usar a terminologia familiar de JOIN do SQL. Esse processo também é chamado de &quot;expansão&quot; do array, mas o termo &quot;join&quot; é usado tanto no nome da função quanto na cláusula porque ele se assemelha a juntar a tabela aos elementos do array, expandindo efetivamente o conjunto de dados de forma semelhante a uma operação JOIN.