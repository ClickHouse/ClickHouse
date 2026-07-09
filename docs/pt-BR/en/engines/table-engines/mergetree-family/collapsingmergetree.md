---
description: 'Herda de MergeTree, mas adiciona lógica de colapsamento de linhas durante o
  processo de mesclagem.'
keywords: ['updates', 'collapsing']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'Motor de tabela CollapsingMergeTree'
doc_type: 'guide'
---

<div id="description">
  ## Descrição
</div>

O motor `CollapsingMergeTree` herda de [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)
e adiciona uma lógica de colapsamento de linhas durante o processo de mesclagem.
O motor de tabela `CollapsingMergeTree` exclui (colapsa) de forma assíncrona
pares de linhas se todos os campos de uma chave de ordenação (`ORDER BY`) forem equivalentes, exceto o campo especial `Sign`,
que pode ter valor `1` ou `-1`.
As linhas sem um par com o valor oposto em `Sign` são mantidas.

Para mais detalhes, consulte a seção [Colapsamento](#table_engine-collapsingmergetree-collapsing) deste documento.

:::note
Este motor pode reduzir significativamente o volume de armazenamento,
aumentando, consequentemente, a eficiência das consultas `SELECT`.
:::

<div id="parameters">
  ## Parâmetros
</div>

Todos os parâmetros deste motor de tabela, com exceção do parâmetro `Sign`,
têm o mesmo significado que em [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree).

* `Sign` — O nome atribuído a uma coluna que indica o tipo de linha, em que `1` representa uma linha de &quot;state&quot; e `-1`, uma linha de &quot;cancel&quot;. Tipo: [Int8](/pt-BR/sql-reference/data-types/int-uint).

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>Método obsoleto para criar uma tabela</summary>

  :::note
  O método abaixo não é recomendado para uso em novos projetos.
  Recomendamos, se possível, atualizar os projetos antigos para usar o novo método.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — Nome dado a uma coluna que indica o tipo de linha, em que `1` é uma linha de &quot;state&quot; e `-1` é uma linha de &quot;cancel&quot;. [Int8](/pt-BR/sql-reference/data-types/int-uint).
</details>

* Para uma descrição dos parâmetros de consulta, consulte a [descrição da consulta](../../../sql-reference/statements/create/table.md).
* Ao criar uma tabela `CollapsingMergeTree`, são necessárias as mesmas [cláusulas da consulta](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) que na criação de uma tabela `MergeTree`.

<div id="table_engine-collapsingmergetree-collapsing">
  ## Colapsamento
</div>

<div id="data">
  ### Dados
</div>

Considere a situação em que você precisa armazenar dados que mudam continuamente para um determinado objeto.
Pode parecer lógico ter uma linha por objeto e atualizá-la sempre que algo mudar,
porém, as operações de atualização são caras e lentas para o SGBD, porque exigem regravar os dados no armazenamento.
Se precisamos gravar dados rapidamente, realizar um grande número de atualizações não é uma abordagem aceitável,
mas sempre podemos gravar sequencialmente as alterações de um objeto.
Para isso, usamos a coluna especial `Sign`.

* Se `Sign` = `1`, isso significa que a linha é uma linha de &quot;state&quot;: *uma linha que contém campos que representam o estado válido atual*.
* Se `Sign` = `-1`, isso significa que a linha é uma linha de &quot;cancel&quot;: *uma linha usada para cancelar o estado de um objeto com os mesmos atributos*.

Por exemplo, queremos calcular quantas páginas os usuários acessaram em um site e por quanto tempo permaneceram nelas.
Em um determinado momento, gravamos a seguinte linha com o estado da atividade do usuário:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Em um momento posterior, registramos a mudança na atividade do usuário e a gravamos nas duas linhas a seguir:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

A primeira linha cancela o estado anterior do objeto (neste caso, representando um usuário).
Ela deve copiar todos os campos da chave de ordenação da linha &quot;cancelada&quot;, exceto `Sign`.
A segunda linha acima contém o estado atual.

Como precisamos apenas do último estado da atividade do usuário, a linha original de &quot;state&quot; e a linha de &quot;cancel&quot;
que inserimos podem ser excluídas, como mostrado abaixo, colapsando o estado inválido (antigo) de um objeto:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` realiza precisamente esse comportamento de *colapsamento* enquanto ocorre a mesclagem das partes de dados.

:::note
O motivo pelo qual são necessárias duas linhas para cada alteração
é discutido em mais detalhes no parágrafo [Algoritmo](#table_engine-collapsingmergetree-collapsing-algorithm).
:::

**As particularidades dessa abordagem**

1. O programa que escreve os dados deve manter o estado de um objeto para poder cancelá-lo. A linha de &quot;cancel&quot; deve conter cópias dos campos da chave de ordenação do &quot;estado&quot; e o `Sign` oposto. Isso aumenta o tamanho inicial do armazenamento, mas permite gravar os dados rapidamente.
2. Arrays longos e crescentes nas colunas reduzem a eficiência do motor devido ao aumento da carga de gravação. Quanto mais simples forem os dados, maior será a eficiência.
3. Os resultados de `SELECT` dependem fortemente da consistência do histórico de alterações do objeto. Seja preciso ao preparar os dados para inserção. Dados inconsistentes podem gerar resultados imprevisíveis. Por exemplo, valores negativos para métricas não negativas, como a profundidade da sessão.

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### Algoritmo
</div>

Quando o ClickHouse faz mesclagem de [partes](/pt-BR/concepts/glossary#parts),
cada grupo de linhas consecutivas com a mesma chave de ordenação (`ORDER BY`) é reduzido a no máximo duas linhas:
a linha de &quot;state&quot; com `Sign` = `1` e a linha de &quot;cancel&quot; com `Sign` = `-1`.
Em outras palavras, no ClickHouse, os registros são colapsados.

Para cada parte de dados resultante, o ClickHouse salva:

|    |                                                                                                                                                                                                              |
| -- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1. | A primeira linha de &quot;cancel&quot; e a última linha de &quot;state&quot;, se o número de linhas de &quot;state&quot; e &quot;cancel&quot; for igual e a última linha for uma linha de &quot;state&quot;. |
| 2. | A última linha de &quot;state&quot;, se houver mais linhas de &quot;state&quot; do que linhas de &quot;cancel&quot;.                                                                                         |
| 3. | A primeira linha de &quot;cancel&quot;, se houver mais linhas de &quot;cancel&quot; do que linhas de &quot;state&quot;.                                                                                      |
| 4. | Nenhuma linha, em todos os demais casos.                                                                                                                                                                     |

Além disso, quando há pelo menos duas linhas de &quot;state&quot; a mais do que linhas de &quot;cancel&quot;
ou pelo menos duas linhas de &quot;cancel&quot; a mais do que linhas de &quot;state&quot;, a mesclagem continua.
No entanto, o ClickHouse trata essa situação como um erro lógico e a registra no log do servidor.
Esse erro pode ocorrer se os mesmos dados forem inseridos mais de uma vez.
Assim, o colapsamento não deve alterar os resultados do cálculo de estatísticas.
As alterações são gradualmente colapsadas para que, no final, reste apenas o último estado de quase todos os objetos.

A coluna `Sign` é necessária porque o algoritmo de mesclagem não garante
que todas as linhas com a mesma chave de ordenação ficarão na mesma parte de dados resultante, nem sequer no mesmo servidor físico.
O ClickHouse processa consultas `SELECT` com múltiplas threads e não consegue prever a ordem das linhas no resultado.

A agregação é necessária se você precisar obter dados totalmente &quot;colapsados&quot; da tabela `CollapsingMergeTree`.
Para concluir o colapsamento, escreva uma consulta com a cláusula `GROUP BY` e funções de agregação que levem o sinal em conta.
Por exemplo, para calcular a quantidade, use `sum(Sign)` em vez de `count()`.
Para calcular a soma de algum valor, use `sum(Sign * x)` junto com `HAVING sum(Sign) > 0` em vez de `sum(x)`
como no [exemplo](#example-of-use) abaixo.

As funções de agregação `count`, `sum` e `avg` podem ser calculadas dessa forma.
A função de agregação `uniq` pode ser calculada se um objeto tiver pelo menos um estado não colapsado.
As funções de agregação `min` e `max` não podem ser calculadas
porque o `CollapsingMergeTree` não salva o histórico dos estados colapsados.

:::note
Se você precisar extrair dados sem agregação
(por exemplo, para verificar se há linhas cujos valores mais recentes correspondem a certas condições),
você pode usar o modificador [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) para a cláusula `FROM`. Ele fará a mesclagem dos dados antes de retornar o resultado.
Para o CollapsingMergeTree, apenas a linha de estado mais recente de cada chave é retornada.
:::

<div id="examples">
  ## Exemplos
</div>

<div id="example-of-use">
  ### Exemplo de uso
</div>

Dados os seguintes dados de exemplo:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Vamos criar uma tabela `UAct` usando o `CollapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Em seguida, vamos inserir alguns dados:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Usamos duas consultas `INSERT` para criar duas partes de dados diferentes.

:::note
Se inserirmos os dados com uma única consulta, o ClickHouse criará apenas uma parte de dados e nunca fará nenhuma mesclagem.
:::

Podemos selecionar os dados usando:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Vamos dar uma olhada nos dados retornados acima e ver se ocorreu colapsamento...
Com duas consultas `INSERT`, criamos duas partes de dados.
A consulta `SELECT` foi executada em duas threads, e obtivemos as linhas em uma ordem aleatória.
No entanto, o colapsamento **não ocorreu** porque ainda não houve a mesclagem das partes de dados
e o ClickHouse mescla partes de dados em segundo plano, em um momento imprevisível que não podemos prever.

Portanto, precisamos de uma agregação,
que realizamos com a função de agregação [`sum`](/pt-BR/sql-reference/aggregate-functions/reference/sum)
e a cláusula [`HAVING`](/pt-BR/sql-reference/statements/select/having):

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Se não precisarmos de agregação e quisermos forçar o colapsamento, também podemos usar o modificador `FINAL` na cláusula `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
Essa forma de selecionar os dados é menos eficiente e não é recomendada para grandes volumes de dados lidos (milhões de linhas).
:::

<div id="example-of-another-approach">
  ### Exemplo de outra abordagem
</div>

A ideia desta abordagem é que as mesclagens levam em conta apenas os campos-chave.
Na linha &quot;cancel&quot;, portanto, podemos especificar valores negativos
que, na soma, compensam a versão anterior da linha sem usar a coluna `Sign`.

Para este exemplo, usaremos os dados de amostra abaixo:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Para essa abordagem, é necessário alterar os tipos de dados de `PageViews` e `Duration` para permitir o armazenamento de valores negativos.
Assim, alteramos o tipo dessas colunas de `UInt8` para `Int16` ao criar a tabela `UAct` usando o
`collapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Vamos testar a abordagem inserindo dados em nossa tabela.

No entanto, para exemplos ou tabelas pequenas, isso é aceitável:

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```