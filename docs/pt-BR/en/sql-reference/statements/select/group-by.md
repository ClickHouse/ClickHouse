---
description: 'Documentação da cláusula GROUP BY'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: 'Cláusula GROUP BY'
doc_type: 'reference'
---

A cláusula `GROUP BY` coloca a consulta `SELECT` no modo de agregação, que funciona da seguinte forma:

* A cláusula `GROUP BY` contém uma lista de expressões (ou uma única expressão, que é considerada uma lista de um item). Essa lista atua como uma &quot;chave de agrupamento&quot;, enquanto cada expressão individual será chamada de &quot;expressão-chave&quot;.
* Todas as expressões nas cláusulas [SELECT](/pt-BR/sql-reference/statements/select/index.md), [HAVING](/pt-BR/sql-reference/statements/select/having.md) e [ORDER BY](/pt-BR/sql-reference/statements/select/order-by.md) **devem** ser calculadas com base em expressões-chave **ou** em [funções de agregação](../../../sql-reference/aggregate-functions/index.md) sobre expressões que não fazem parte da chave (incluindo colunas simples). Em outras palavras, cada coluna selecionada da tabela deve ser usada ou em uma expressão-chave ou dentro de uma função de agregação, mas não em ambas.
* O resultado da agregação da consulta `SELECT` conterá tantas linhas quantos forem os valores únicos da &quot;chave de agrupamento&quot; na tabela de origem. Normalmente, isso reduz significativamente o número de linhas, muitas vezes em ordens de grandeza, mas não necessariamente: o número de linhas permanece o mesmo se todos os valores da &quot;chave de agrupamento&quot; forem distintos.

Quando quiser agrupar dados na tabela por números de colunas em vez de nomes de colunas, habilite a configuração [enable&#95;positional&#95;arguments](/pt-BR/operations/settings/settings#enable_positional_arguments).

:::note
Há outra forma de executar a agregação em uma tabela. Se uma consulta contiver colunas da tabela apenas dentro de funções de agregação, a cláusula `GROUP BY` pode ser omitida, e a agregação por um conjunto vazio de chaves é assumida. Essas consultas sempre retornam exatamente uma linha.
:::

<div id="null-processing">
  ## Processamento de NULL
</div>

Em agrupamentos, o ClickHouse interpreta [NULL](/pt-BR/sql-reference/syntax#null) como um valor, e `NULL==NULL`. Isso difere do processamento de `NULL` na maioria dos outros contextos.

Veja um exemplo do que isso significa.

Suponha que você tenha esta tabela:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

A consulta `SELECT sum(x), y FROM t_null_big GROUP BY y` resulta em:

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Você pode ver que o `GROUP BY` para `y = NULL` somou `x`, como se `NULL` fosse esse valor.

Se você passar várias colunas para o `GROUP BY`, o resultado mostrará todas as combinações da seleção, como se `NULL` fosse um valor específico.

<div id="rollup-modifier">
  ## Modificador ROLLUP
</div>

O modificador `ROLLUP` é usado para calcular subtotais para as expressões-chave, com base na ordem delas na lista `GROUP BY`. As linhas de subtotais são adicionadas após a tabela de resultados.

Os subtotais são calculados em ordem inversa: primeiro, são calculados os subtotais para a última expressão-chave da lista; depois, para a anterior, e assim por diante até a primeira expressão-chave.

Nas linhas de subtotais, os valores das expressões-chave já &quot;agrupadas&quot; são definidos como `0` ou string vazia.

:::note
Observe que a cláusula [HAVING](/pt-BR/sql-reference/statements/select/having.md) pode afetar os resultados dos subtotais.
:::

**Exemplo**

Considere a tabela t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```

Como a seção `GROUP BY` tem três expressões-chave, o resultado contém quatro tabelas com subtotais &quot;acumulados&quot; da direita para a esquerda:

* `GROUP BY year, month, day`;
* `GROUP BY year, month` (e a coluna `day` é preenchida com zeros);
* `GROUP BY year` (agora as colunas `month` e `day` são preenchidas com zeros);
* e os totais (e as três colunas de expressões-chave são zero).

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

A mesma consulta também pode ser escrita com a palavra-chave `WITH`.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**Veja também**

* Configuração [group&#95;by&#95;use&#95;nulls](/pt-BR/operations/settings/settings.md#group_by_use_nulls) para compatibilidade com o padrão SQL.

<div id="cube-modifier">
  ## Modificador CUBE
</div>

O modificador `CUBE` é usado para calcular subtotais para cada combinação das expressões-chave na lista `GROUP BY`. As linhas de subtotais são adicionadas após a tabela de resultados.

Nas linhas de subtotais, os valores de todas as expressões-chave &quot;agrupadas&quot; são definidos como `0` ou vazios.

:::note
Observe que a cláusula [HAVING](/pt-BR/sql-reference/statements/select/having.md) pode afetar os resultados dos subtotais.
:::

**Exemplo**

Considere a tabela t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

Como a cláusula `GROUP BY` tem três expressões-chave, o resultado contém oito tabelas com subtotais para todas as combinações de expressões-chave:

* `GROUP BY year, month, day`
* `GROUP BY year, month`
* `GROUP BY year, day`
* `GROUP BY year`
* `GROUP BY month, day`
* `GROUP BY month`
* `GROUP BY day`
* e os totais.

As colunas excluídas de `GROUP BY` são preenchidas com zeros.

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

A mesma consulta também pode ser escrita com a palavra-chave `WITH`.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**Veja também**

* Configuração [group&#95;by&#95;use&#95;nulls](/pt-BR/operations/settings/settings.md#group_by_use_nulls) para compatibilidade com o padrão SQL.

<div id="with-totals-modifier">
  ## Modificador WITH TOTALS
</div>

Se o modificador `WITH TOTALS` for especificado, outra linha será calculada. Essa linha terá colunas-chave contendo valores padrão (zeros ou valores vazios) e colunas de funções de agregação com os valores calculados em todas as linhas (os valores &quot;totais&quot;).

Essa linha extra só é produzida nos formatos `JSON*`, `TabSeparated*` e `Pretty*`, separadamente das outras linhas:

* Nos formatos `XML` e `JSON*`, essa linha é retornada como um campo `totals` separado.
* Nos formatos `TabSeparated*`, `CSV*` e `Vertical`, a linha vem após o resultado principal, precedida por uma linha vazia (depois dos outros dados).
* Nos formatos `Pretty*`, a linha é retornada como uma tabela separada após o resultado principal.
* No formato `Template`, a linha é retornada de acordo com o modelo especificado.
* Nos outros formatos, ela não está disponível.

:::note
totals é retornado nos resultados de consultas `SELECT` e não é retornado em `INSERT INTO ... SELECT`.
:::

`WITH TOTALS` pode se comportar de maneiras diferentes quando [HAVING](/pt-BR/sql-reference/statements/select/having.md) está presente. O comportamento depende da configuração `totals_mode`.

<div id="configuring-totals-processing">
  ### Configurando o processamento de totals
</div>

Por padrão, `totals_mode = 'before_having'`. Nesse caso, &#39;totals&#39; é calculado sobre todas as linhas, inclusive as que não passam por HAVING e `max_rows_to_group_by`.

As outras alternativas incluem em &#39;totals&#39; apenas as linhas que passam por HAVING e se comportam de forma diferente com a configuração `max_rows_to_group_by` e `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Não inclui as linhas que não passaram por `max_rows_to_group_by`. Em outras palavras, &#39;totals&#39; terá menos linhas ou o mesmo número de linhas que teria se `max_rows_to_group_by` fosse omitido.

`after_having_inclusive` – Inclui em &#39;totals&#39; todas as linhas que não passaram por `max_rows_to_group_by`. Em outras palavras, &#39;totals&#39; terá mais linhas ou o mesmo número de linhas que teria se `max_rows_to_group_by` fosse omitido.

`after_having_auto` – Conta o número de linhas que passaram por HAVING. Se ele for maior que um determinado valor (por padrão, 50%), inclui em &#39;totals&#39; todas as linhas que não passaram por `max_rows_to_group_by`. Caso contrário, não as inclui.

`totals_auto_threshold` – Por padrão, 0.5. O coeficiente de `after_having_auto`.

Se `max_rows_to_group_by` e `group_by_overflow_mode = 'any'` não forem usados, todas as variações de `after_having` serão iguais, e você poderá usar qualquer uma delas (por exemplo, `after_having_auto`).

Você pode usar `WITH TOTALS` em subconsultas, incluindo subconsultas na cláusula [JOIN](/pt-BR/sql-reference/statements/select/join.md) (nesse caso, os respectivos valores totais são combinados).

<div id="group-by-all">
  ## GROUP BY ALL
</div>

`GROUP BY ALL` equivale a listar todas as expressões do `SELECT` que não são funções de agregação.

Por exemplo:

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

é igual a

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

Em um caso especial, se houver uma função que tenha tanto funções de agregação quanto outros campos como argumentos, as chaves de `GROUP BY` conterão o máximo possível de campos não agregados que pudermos extrair dela.

Por exemplo:

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

é o mesmo que

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

<div id="examples">
  ## Exemplos
</div>

Exemplo:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Ao contrário do MySQL (e em conformidade com o SQL padrão), você não pode obter um valor de uma coluna que não esteja em uma chave nem em uma função de agregação (exceto expressões constantes). Para contornar isso, você pode usar a função de agregação &#39;any&#39; (obtém o primeiro valor encontrado) ou &#39;min/max&#39;.

Exemplo:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Para cada valor de chave distinto encontrado, `GROUP BY` calcula um conjunto de valores de funções de agregação.

<div id="grouping-sets-modifier">
  ## modificador GROUPING SETS
</div>

Este é o modificador mais geral.
Esse modificador permite especificar manualmente vários conjuntos de chaves de agregação (grouping sets).
A agregação é executada separadamente para cada grouping set e, em seguida, todos os resultados são combinados.
Se uma coluna não estiver presente em um grouping set, ela será preenchida com um valor padrão.

Em outras palavras, os modificadores descritos acima podem ser representados por `GROUPING SETS`.
Embora consultas com os modificadores `ROLLUP`, `CUBE` e `GROUPING SETS` sejam sintaticamente equivalentes, elas podem ter desempenhos diferentes.
Enquanto `GROUPING SETS` tenta executar tudo em paralelo, `ROLLUP` e `CUBE` executam a mesclagem final dos agregados em uma única thread.

Quando as colunas de origem contêm valores padrão, pode ser difícil distinguir se uma linha faz parte da agregação que usa essas colunas como chaves ou não.
Para resolver esse problema, a função `GROUPING` deve ser usada.

**Exemplo**

As duas consultas a seguir são equivalentes.

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**Veja também**

* a configuração [group&#95;by&#95;use&#95;nulls](/pt-BR/operations/settings/settings.md#group_by_use_nulls) para compatibilidade com o padrão SQL.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

A agregação é um dos recursos mais importantes de um DBMS orientado a colunas e, por isso, sua implementação é uma das partes mais otimizadas do ClickHouse. Por padrão, a agregação é feita em memória usando uma tabela hash. Ela tem mais de 40 especializações, escolhidas automaticamente de acordo com os tipos de dados da &quot;chave de agrupamento&quot;.

<div id="group-by-optimization-depending-on-table-sorting-key">
  ### Otimização de `GROUP BY` dependendo da chave de ordenação da tabela
</div>

A agregação pode ser realizada com mais eficiência se a tabela estiver ordenada por alguma chave e a expressão `GROUP BY` contiver pelo menos o prefixo da chave de ordenação ou funções injetivas. Nesse caso, quando uma nova chave é lida da tabela, o resultado intermediário da agregação pode ser finalizado e enviado ao client. Esse comportamento é ativado pela configuração [optimize&#95;aggregation&#95;in&#95;order](../../../operations/settings/settings.md#optimize_aggregation_in_order). Essa otimização reduz o uso de memória durante a agregação, mas, em alguns casos, pode tornar a execução da consulta mais lenta.

<div id="group-by-in-external-memory">
  ### GROUP BY em Memória Externa
</div>

Você pode ativar a gravação de dados temporários em disco para limitar o uso de memória durante o `GROUP BY`.
A configuração [max&#95;bytes&#95;before&#95;external&#95;group&#95;by](/pt-BR/operations/settings/settings#max_bytes_before_external_group_by) define o limite de consumo de RAM para gravar em disco os dados temporários do `GROUP BY`. Se for definida como 0 (o padrão), ela ficará desabilitada.
Como alternativa, você pode definir [max&#95;bytes&#95;ratio&#95;before&#95;external&#95;group&#95;by](/pt-BR/operations/settings/settings#max_bytes_ratio_before_external_group_by), que permite usar `GROUP BY` em memória externa somente quando a consulta atingir um determinado limite de uso de memória.

Ao usar `max_bytes_before_external_group_by`, recomendamos definir `max_memory_usage` com um valor aproximadamente duas vezes maior (ou `max_bytes_ratio_before_external_group_by=0.5`). Isso é necessário porque há dois estágios na agregação: leitura dos dados e formação dos dados intermediários (1), e mesclagem dos dados intermediários (2). A gravação de dados no sistema de arquivos só pode ocorrer durante o estágio 1. Se os dados temporários não tiverem sido gravados, o estágio 2 poderá exigir até a mesma quantidade de memória que o estágio 1.

Por exemplo, se [max&#95;memory&#95;usage](/pt-BR/operations/settings/settings#max_memory_usage) estiver definido como 10000000000 e você quiser usar agregação externa, faz sentido definir `max_bytes_before_external_group_by` como 10000000000 e `max_memory_usage` como 20000000000. Quando a agregação externa é acionada (se tiver havido pelo menos uma gravação de dados temporários), o consumo máximo de RAM fica apenas um pouco acima de `max_bytes_before_external_group_by`.

Com o processamento distribuído de consultas, a agregação externa é realizada em servidores remotos. Para que o servidor solicitante use apenas uma pequena quantidade de RAM, defina `distributed_aggregation_memory_efficient` como 1.

A mesclagem de dados gravados em disco, assim como a mesclagem de resultados de servidores remotos quando a configuração `distributed_aggregation_memory_efficient` está habilitada, consome até `1/256 * the_number_of_threads` da quantidade total de RAM.

Quando a agregação externa está habilitada, se houver menos de `max_bytes_before_external_group_by` em dados (ou seja, os dados não foram gravados), a consulta será executada tão rapidamente quanto sem agregação externa. Se algum dado temporário tiver sido gravado, o tempo de execução será várias vezes maior (aproximadamente três vezes).

Se você tiver um [ORDER BY](/pt-BR/sql-reference/statements/select/order-by.md) com um [LIMIT](/pt-BR/sql-reference/statements/select/limit.md) após o `GROUP BY`, a quantidade de RAM usada dependerá da quantidade de dados em `LIMIT`, e não na tabela inteira. Mas, se o `ORDER BY` não tiver `LIMIT`, não se esqueça de ativar a ordenação externa (`max_bytes_before_external_sort`).