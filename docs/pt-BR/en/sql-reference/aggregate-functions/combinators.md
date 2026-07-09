---
description: 'Documentação sobre combinadores de funções de agregação'
sidebar_label: 'Combinadores'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: 'Combinadores de funções de agregação'
doc_type: 'reference'
---

O nome de uma função de agregação pode receber um sufixo. Isso altera a forma como ela funciona.

<div id="-if">
  ## -If
</div>

O sufixo -If pode ser acrescentado ao nome de qualquer função de agregação. Nesse caso, a função de agregação aceita um argumento extra — uma condição (tipo Uint8). A função de agregação processa apenas as linhas que satisfazem a condição. Se a condição não for satisfeita nenhuma vez, ela retornará um valor padrão (geralmente zeros ou strings vazias).

Exemplos: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` e assim por diante.

Com funções de agregação condicionais, você pode calcular agregados para várias condições ao mesmo tempo, sem usar subconsultas e `JOIN`s. Por exemplo, funções de agregação condicionais podem ser usadas para implementar a funcionalidade de comparação de segmentos.

<div id="-array">
  ## -Array
</div>

O sufixo -Array pode ser anexado a qualquer função de agregação. Nesse caso, a função de agregação recebe argumentos do tipo &#39;Array(T)&#39; (arrays), em vez de argumentos do tipo &#39;T&#39;. Se a função de agregação aceitar vários argumentos, eles devem ser arrays de mesmo comprimento. Ao processar arrays, a função de agregação funciona como a função de agregação original sobre todos os elementos do array.

Exemplo 1: `sumArray(arr)` - Soma todos os elementos de todos os arrays &#39;arr&#39;. Neste exemplo, isso poderia ser escrito de forma mais simples: `sum(arraySum(arr))`.

Exemplo 2: `uniqArray(arr)` – Conta o número de elementos únicos em todos os arrays &#39;arr&#39;. Isso também pode ser feito de uma forma mais simples: `uniq(arrayJoin(arr))`, mas nem sempre é possível adicionar `arrayJoin` a uma consulta.

-If e -Array podem ser combinados. No entanto, &#39;Array&#39; deve vir primeiro, seguido de &#39;If&#39;. Exemplos: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Devido a essa ordem, o argumento &#39;cond&#39; não será um array.

<div id="-map">
  ## -Map
</div>

O sufixo -Map pode ser adicionado a qualquer função de agregação. Isso cria uma função de agregação que recebe o tipo Map como argumento e agrega separadamente os valores de cada chave do map usando a função de agregação especificada. O resultado também é do tipo Map.

**Exemplo**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

Se aplicar este combinador, a função de agregação retornará o mesmo valor, mas com um tipo diferente. Trata-se de uma [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) que pode ser armazenada em uma tabela para uso com tabelas [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

**Sintaxe**

```sql
<aggFunction>SimpleState(x)
```

**Argumentos**

* `x` — Parâmetros da função de agregação.

**Valores retornados**

O valor de uma função de agregação do tipo `SimpleAggregateFunction(...)`.

**Exemplo**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

Se você aplicar este combinador, a função de agregação não retornará o valor resultante (como o número de valores únicos da função [uniq](/pt-BR/sql-reference/aggregate-functions/reference/uniq)), mas um estado intermediário da agregação (para `uniq`, trata-se da tabela hash usada para calcular o número de valores únicos). Isso é um `AggregateFunction(...)` que pode ser usado para processamento adicional ou armazenado em uma tabela para concluir a agregação posteriormente.

:::note
Observe que -MapState não é um invariante para os mesmos dados, pois a ordem dos dados no estado intermediário pode mudar, embora isso não afete a ingestão desses dados.
:::

Para trabalhar com esses estados, use:

* motor de tabela [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).
* função [finalizeAggregation](/pt-BR/sql-reference/functions/other-functions#finalizeAggregation).
* função [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate).
* combinador [-Merge](#-merge).
* combinador [-MergeState](#-mergestate).

<div id="-merge">
  ## -Merge
</div>

Ao aplicar esse combinador, a função de agregação recebe o estado intermediário de agregação como argumento, combina os estados para concluir a agregação e retorna o valor resultante.

<div id="-mergestate">
  ## -MergeState
</div>

Mescla os estados intermediários de agregação da mesma forma que o combinador -Merge. No entanto, ele não retorna o valor resultante, mas um estado intermediário de agregação, semelhante ao combinador -State.

<div id="-foreach">
  ## -ForEach
</div>

Converte uma função de agregação para tabelas em uma função de agregação para arrays que agrega os itens correspondentes dos arrays e retorna um array de resultados. Por exemplo, `sumForEach` para os arrays `[1, 2]`, `[3, 4, 5]`e`[6, 7]`retorna o resultado `[10, 13, 5]` após somar os itens correspondentes.

<div id="-tuple">
  ## -Tuple
</div>

O sufixo `-Tuple` pode ser acrescentado a qualquer função de agregação. A função resultante recebe um argumento do tipo `Tuple` para cada argumento da função de agregação subjacente; todas as tuplas devem ter o mesmo número de elementos. A agregação é aplicada de forma independente a cada posição dos elementos, recebendo o elemento correspondente de cada `Tuple`, e retorna uma `Tuple` de resultados.

Se a primeira `Tuple` de entrada tiver nomes de elementos explícitos, eles serão preservados no resultado.

Funções de agregação que processam valores `NULL` por conta própria (`anyRespectNulls`, `anyLastRespectNulls`, o modificador `RESPECT NULLS`) não oferecem suporte ao tipo `Nullable(Tuple(...))` como argumento; use elementos `Nullable`.

**Sintaxe**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**Argumentos**

* `tuple1[, tuple2, ...]` — Colunas do tipo `Tuple`, uma para cada argumento da função de agregação subjacente, todas com o mesmo número de elementos. Cada elemento deve ser de um tipo aceito pela função de agregação subjacente naquela posição do argumento.

**Valores retornados**

* Um `Tuple` contendo o resultado da aplicação da função de agregação a cada elemento de forma independente.

Tipo: `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**Exemplo**

Consulta:

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

Resultado:

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

Usando `GROUP BY`:

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

Uso com uma função de agregação com múltiplos argumentos: cada argumento `Tuple` fornece um argumento da função subjacente, e os elementos são combinados de acordo com a posição:

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` e `b1` são anticorrelacionados, enquanto `a2` e `b2` são proporcionais, então o resultado é `(-1, 1)`.

`-Tuple` pode ser combinado com outros combinadores, como `-If`. Por exemplo: `sumTupleIf(tuple_column, cond)`.

<div id="-distinct">
  ## -Distinct
</div>

Cada combinação única de argumentos será agregada apenas uma vez. Valores repetidos são ignorados.
Exemplos: `sum(DISTINCT x)` (ou `sumDistinct(x)`), `groupArray(DISTINCT x)` (ou `groupArrayDistinct(x)`), `corrStable(DISTINCT x, y)` (ou `corrStableDistinct(x, y)`) e assim por diante.

<div id="-ordefault">
  ## -OrDefault
</div>

Altera o comportamento de uma função de agregação.

Se uma função de agregação não tiver valores de entrada, com este combinador ela retornará o valor padrão do tipo de dado de retorno. Aplica-se a funções de agregação que podem receber dados de entrada vazios.

`-OrDefault` pode ser usado com outros combinadores.

**Sintaxe**

```sql
<aggFunction>OrDefault(x)
```

**Argumentos**

* `x` — Parâmetros da função de agregação.

**Valores retornados**

Retorna o valor padrão do tipo de retorno de uma função de agregação se não houver nada a agregar.

O tipo depende da função de agregação usada.

**Exemplo**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Além disso, `-OrDefault` também pode ser usado com outros combinadores. Isso é útil quando a função de agregação não aceita entrada vazia.

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

Altera o comportamento de uma função de agregação.

Este combinador converte o resultado de uma função de agregação para o tipo de dado [Nullable](../../sql-reference/data-types/nullable.md). Se a função de agregação não tiver valores para processar, ela retorna [NULL](/pt-BR/operations/settings/formats#input_format_null_as_default).

`-OrNull` pode ser usado com outros combinadores.

**Sintaxe**

```sql
<aggFunction>OrNull(x)
```

**Argumentos**

* `x` — Parâmetros da função de agregação.

**Valores retornados**

* O resultado da função de agregação, convertido para o tipo de dado `Nullable`.
* `NULL`, se não houver nada para agregar.

Tipo: `Nullable(tipo de retorno da função de agregação)`.

**Exemplo**

Adicione `-orNull` ao final do nome da função de agregação.

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

Além disso, `-OrNull` também pode ser usado com outros combinadores. Isso é útil quando a função de agregação não aceita entrada vazia.

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Reamostragem
</div>

Permite dividir os dados em grupos e, em seguida, agregá-los separadamente. Os grupos são criados dividindo os valores de uma coluna em intervalos.

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Argumentos**

* `start` — Valor inicial do intervalo completo para os valores de `resampling_key`.
* `stop` — Valor final do intervalo completo para os valores de `resampling_key`. O intervalo completo não inclui o valor `stop` `[start, stop)`.
* `step` — Passo para dividir o intervalo completo em subintervalos. A `aggFunction` é executada em cada um desses subintervalos de forma independente.
* `resampling_key` — Coluna cujos valores são usados para separar os dados em intervalos.
* `aggFunction_params` — Parâmetros de `aggFunction`.

**Valores retornados**

* Array de resultados de `aggFunction` para cada subintervalo.

**Exemplo**

Considere a tabela `people` com os seguintes dados:

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Vamos obter os nomes das pessoas cuja idade está nos intervalos `[30,60)` e `[60,75)`. Como usamos uma representação inteira para a idade, obtemos idades nos intervalos `[30, 59]` e `[60,74]`.

Para agregar nomes em um Array, usamos a função agregadora [groupArray](/pt-BR/sql-reference/aggregate-functions/reference/grouparray). Ela recebe um argumento. No nosso caso, é a coluna `name`. A função `groupArrayResample` deve usar a coluna `age` para agregar os nomes por idade. Para definir os intervalos necessários, passamos os argumentos `30, 75, 30` para a função `groupArrayResample`.

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Considere os resultados.

`John` fica fora da amostra porque é jovem demais. As outras pessoas são distribuídas de acordo com os intervalos de idade especificados.

Agora vamos contar o número total de pessoas e o salário médio delas nos intervalos de idade especificados.

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

O sufixo -ArgMin pode ser adicionado ao nome de qualquer função de agregação. Nesse caso, a função de agregação aceita um argumento adicional, que deve ser uma expressão comparável. A função de agregação processa apenas as linhas que têm o valor mínimo da expressão adicional especificada.

Exemplos: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` e assim por diante.

<div id="-argmax">
  ## -ArgMax
</div>

Semelhante ao sufixo -ArgMin, mas processa apenas as linhas com o valor máximo da expressão extra especificada.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Como usar combinadores de agregação no ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)