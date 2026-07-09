---
description: 'Documentação para funções regulares'
sidebar_label: 'Visão geral'
sidebar_position: 1
slug: /sql-reference/functions/overview
title: 'Funções regulares'
doc_type: 'referência'
---

Há pelo menos* dois tipos de funções: funções regulares (chamadas simplesmente de &quot;funções&quot;) e funções de agregação. Esses são conceitos completamente diferentes. As funções regulares funcionam como se fossem aplicadas a cada linha separadamente (para cada linha, o resultado da função não depende das outras linhas). As funções de agregação acumulam um conjunto de valores de várias linhas (ou seja, dependem do conjunto completo de linhas).

Nesta seção, discutimos as funções regulares. Para funções de agregação, consulte a seção &quot;Funções de agregação&quot;.

:::note
Há um terceiro tipo de função, ao qual a função [&#39;arrayJoin&#39;](../functions/array-join.md) pertence. E as [funções de tabela](../table-functions/index.md) também podem ser mencionadas separadamente.
:::

<div id="strong-typing">
  ## Tipagem forte
</div>

Ao contrário do SQL padrão, o ClickHouse tem tipagem forte. Em outras palavras, ele não faz conversões implícitas entre tipos. Cada função funciona com um conjunto específico de tipos. Isso significa que, às vezes, é preciso usar funções de conversão.

<div id="common-subexpression-elimination">
  ## Eliminação de subexpressões comuns
</div>

Todas as expressões em uma consulta que têm a mesma AST (o mesmo registro ou o mesmo resultado da análise sintática) são consideradas como tendo valores idênticos. Essas expressões são agrupadas e executadas uma única vez. Subconsultas idênticas também são eliminadas dessa mesma forma.

<div id="types-of-results">
  ## Tipos de resultados
</div>

Todas as funções retornam um único valor como resultado (não vários valores nem nenhum valor). O tipo do resultado geralmente é definido apenas pelos tipos dos argumentos, não pelos valores. As exceções são a função tupleElement (o operador a.N) e a função toFixedString.

<div id="constants">
  ## Constantes
</div>

Por simplicidade, certas funções só funcionam com constantes em alguns argumentos. Por exemplo, o argumento à direita do operador LIKE deve ser uma constante.
Quase todas as funções retornam uma constante para argumentos constantes. A exceção são as funções que geram números aleatórios.
A função &#39;now&#39; retorna valores diferentes para consultas executadas em momentos distintos, mas o resultado é considerado uma constante, já que a constância só é importante dentro de uma única consulta.
Uma expressão constante também é considerada uma constante (por exemplo, o lado direito do operador LIKE pode ser construído a partir de várias constantes).

As funções podem ser implementadas de formas diferentes para argumentos constantes e não constantes (código diferente é executado). Mas os resultados de uma constante e de uma coluna propriamente dita que contenha apenas o mesmo valor devem ser equivalentes.

<div id="null-processing">
  ## Tratamento de NULL
</div>

As funções têm os seguintes comportamentos:

* Se pelo menos um dos argumentos da função for `NULL`, o resultado da função também será `NULL`.
* Comportamento especial, especificado individualmente na descrição de cada função. No código-fonte do ClickHouse, essas funções têm `UseDefaultImplementationForNulls=false`.

<div id="constancy">
  ## Constância
</div>

As funções não podem alterar os valores de seus argumentos — qualquer alteração é retornada como resultado. Assim, o resultado do cálculo de funções separadas não depende da ordem em que elas são escritas na consulta.

<div id="higher-order-functions">
  ## Funções de ordem superior
</div>

<div id="arrow-operator-and-lambda">
  ### Operador `->` e funções lambda(params, expr)
</div>

Funções de ordem superior só aceitam funções lambda como argumento de função. Para passar uma função lambda a uma função de ordem superior, use o operador `->`. À esquerda da seta, há um parâmetro formal, que pode ser qualquer ID, ou vários parâmetros formais — quaisquer IDs em uma tupla. À direita da seta, há uma expressão que pode usar esses parâmetros formais, bem como quaisquer colunas da tabela.

Exemplos:

```python
x -> 2 * x
str -> str != Referer
```

Uma função lambda que aceita vários argumentos também pode ser passada para uma função de ordem superior. Nesse caso, a função de ordem superior recebe vários arrays de mesmo comprimento, aos quais esses argumentos correspondem.

Em algumas funções, o primeiro argumento (a função lambda) pode ser omitido. Nesse caso, presume-se um mapeamento de identidade.

<div id="bare-function-names-as-lambdas">
  ### Nomes de funções simples como lambdas
</div>

Em vez de escrever uma expressão lambda completa, você pode passar o nome de uma função diretamente para uma função de ordem superior. O nome da função é convertido automaticamente em uma expressão lambda equivalente.

Por exemplo, os pares a seguir são equivalentes:

```sql
SELECT arrayMap(negate, [1, 2, 3]);            -- [-1, -2, -3]
SELECT arrayMap(x -> negate(x), [1, 2, 3]);    -- [-1, -2, -3]

SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);            -- [11, 22, 33]
SELECT arrayMap((x, y) -> plus(x, y), [1, 2, 3], [10, 20, 30]); -- [11, 22, 33]

SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);            -- [1, 3, 5]
SELECT arrayFilter(x -> isNotNull(x), [1, NULL, 3, NULL, 5]);    -- [1, 3, 5]

SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));                      -- 15
SELECT arrayFold((acc, x) -> plus(acc, x), [1, 2, 3, 4, 5], toUInt64(0));  -- 15
```

Isso funciona com funções integradas, SQL UDFs, executable UDFs e WebAssembly UDFs. Nomes de coluna e aliases têm prioridade sobre nomes de funções quando há ambiguidade.

A aridade da lambda é determinada pela função interna. Por exemplo, `arrayMap(plus, ...)` usa aridade 2 porque `plus` recebe dois argumentos, então também funciona com entradas de tupla, como `arrayMap(plus, [(1, 10), (2, 20)])`, em que os elementos da tupla são desempacotados nos argumentos da lambda.

Para funções internas variádicas (como `concat`, que aceita qualquer número de argumentos), a aridade da lambda passa a ser determinada pelo número de argumentos de array. Isso está correto para funções de ordem superior como `arrayMap`, `arrayFilter` e `arrayFold`. Para funções de ordem superior que aceitam parâmetros fixos não array além de arrays — por exemplo, `arrayPartialSort(f, limit, arr)` — nomes simples de funções variádicas podem produzir a aridade errada; nesse caso, é necessária uma lambda explícita.

Funções internas variádicas também não desempacotam automaticamente entradas de tupla. Por exemplo, `arrayMap(concat, [('a', 'b'), ('c', 'd')])` é reescrita como uma lambda unária e não é equivalente a `arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')])`. Use uma lambda explícita quando quiser desestruturar elementos de tupla em uma chamada variádica.

<div id="user-defined-functions-udfs">
  ## Funções Definidas pelo Usuário (UDFs)
</div>

O ClickHouse é compatível com funções definidas pelo usuário. Consulte [UDFs](../functions/udf.md).