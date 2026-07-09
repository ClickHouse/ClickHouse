---
description: 'Documentação para o tipo de dado SimpleAggregateFunction'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'Tipo de dado SimpleAggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Descrição
</div>

O tipo de dados `SimpleAggregateFunction` armazena o estado intermediário de uma
função de agregação, mas não seu estado completo, como ocorre com o tipo
[`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md).

Essa otimização pode ser aplicada a funções para as quais vale a seguinte propriedade:

> o resultado da aplicação de uma função `f` a um conjunto de linhas `S1 UNION ALL S2` pode
> ser obtido aplicando `f` separadamente a partes do conjunto de linhas e, em seguida,
> aplicando `f` novamente aos resultados: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

Essa propriedade garante que os resultados da agregação parcial são suficientes para calcular
o resultado combinado, portanto não precisamos armazenar nem processar dados adicionais. Por
exemplo, o resultado das funções `min` ou `max` não exige etapas extras para
calcular o resultado final a partir das etapas intermediárias, enquanto a função `avg`
exige manter uma soma e uma contagem, que serão divididas para obter a
média em uma etapa final de `Merge`, que combina os estados intermediários.

Os valores de funções de agregação geralmente são produzidos chamando uma função de agregação
com o combinador [`-SimpleState`](/pt-BR/sql-reference/aggregate-functions/combinators#-simplestate) anexado ao nome da função.

<div id="syntax">
  ## Sintaxe
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parâmetros**

* `aggregate_function_name` - O nome de uma função de agregação.
* `Tipo` - Os tipos dos argumentos da função de agregação.

<div id="supported-functions">
  ## Funções suportadas
</div>

As seguintes funções de agregação são suportadas:

* [`any`](/pt-BR/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/pt-BR/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/pt-BR/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/pt-BR/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/pt-BR/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/pt-BR/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/pt-BR/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/pt-BR/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/pt-BR/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/pt-BR/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/pt-BR/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/pt-BR/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/pt-BR/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/pt-BR/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/pt-BR/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
Os valores de `SimpleAggregateFunction(func, Type)` têm o mesmo `Type`,
portanto, diferentemente do tipo `AggregateFunction`, não é necessário aplicar
os combinadores `-Merge`/`-State`.

O tipo `SimpleAggregateFunction` tem melhor desempenho que `AggregateFunction`
para as mesmas funções de agregação.
:::

<div id="example">
  ## Exemplo
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Uso de combinators de agregação no ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - Blog: [Uso de combinators de agregação no ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* Tipo [AggregateFunction](/pt-BR/sql-reference/data-types/aggregatefunction).