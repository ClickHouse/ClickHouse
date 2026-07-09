---
description: 'Permite a gravação rápida de estados de objetos que mudam continuamente,
  e a exclusão de estados antigos de objetos em segundo plano.'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'motor de tabela VersionedCollapsingMergeTree'
doc_type: 'referência'
---

Este motor:

* Permite a gravação rápida de estados de objetos que mudam continuamente.
* Exclui estados antigos de objetos em segundo plano. Isso reduz significativamente o volume de armazenamento.

Consulte a seção [Colapsamento](#table_engines_versionedcollapsingmergetree) para mais detalhes.

O motor herda de [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree) e adiciona a lógica de colapso de linhas ao algoritmo de mesclagem de partes de dados. `VersionedCollapsingMergeTree` tem a mesma finalidade que [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md), mas usa um algoritmo de colapso diferente que permite inserir dados em qualquer ordem com múltiplas threads. Em particular, a coluna `Version` ajuda a colapsar as linhas corretamente, mesmo que sejam inseridas fora de ordem. Em contraste, `CollapsingMergeTree` permite apenas inserções estritamente consecutivas.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para ver uma descrição dos parâmetros de consulta, consulte a [descrição da consulta](../../../sql-reference/statements/create/table.md).

<div id="engine-parameters">
  ### Parâmetros do motor
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| Parâmetro | Descrição                                                                                                         | Tipo                                                                                                                                                                                                                                                                                          |
| --------- | ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | Nome da coluna com o tipo de linha: `1` é uma linha de &quot;state&quot;, `-1` é uma linha de &quot;cancel&quot;. | [`Int8`](/pt-BR/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                  |
| `version` | Nome da coluna com a versão do estado do objeto.                                                                  | [`Int*`](/pt-BR/sql-reference/data-types/int-uint), [`UInt*`](/pt-BR/sql-reference/data-types/int-uint), [`Date`](/pt-BR/sql-reference/data-types/date), [`Date32`](/pt-BR/sql-reference/data-types/date32), [`DateTime`](/pt-BR/sql-reference/data-types/datetime) ou [`DateTime64`](/pt-BR/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### Cláusulas da consulta
</div>

Ao criar uma tabela `VersionedCollapsingMergeTree`, são exigidas as mesmas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) que na criação de uma tabela `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para criar uma tabela</summary>

  :::note
  Não use este método em novos projetos. Se possível, migre os projetos antigos para o método descrito acima.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  Todos os parâmetros, exceto `sign` e `version`, têm o mesmo significado que em `MergeTree`.

  * `sign` — Nome da coluna com o tipo de linha: `1` é uma linha de &quot;state&quot; e `-1` é uma linha de &quot;cancel&quot;.

    Tipo de dados da coluna — `Int8`.

  * `version` — Nome da coluna com a versão do estado do objeto.

    O tipo de dados da coluna deve ser `UInt*`.
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## Colapsamento
</div>

<div id="data">
  ### Dados
</div>

Considere uma situação em que você precisa salvar dados que mudam continuamente para algum objeto. É razoável ter uma linha para um objeto e atualizá-la sempre que houver alterações. No entanto, a operação de atualização é cara e lenta para um SGBD, porque exige regravar os dados no armazenamento. A atualização não é aceitável se você precisar gravar dados rapidamente, mas é possível gravar as alterações de um objeto de forma sequencial, como a seguir.

Use a coluna `Sign` ao gravar a linha. Se `Sign = 1`, isso significa que a linha representa um estado do objeto (vamos chamá-la de linha de &quot;estado&quot;). Se `Sign = -1`, isso indica o cancelamento do estado de um objeto com os mesmos atributos (vamos chamá-la de linha de &quot;cancelamento&quot;). Use também a coluna `Version`, que deve identificar cada estado de um objeto com um número distinto.

Por exemplo, queremos calcular quantas páginas os usuários visitaram em um site e por quanto tempo permaneceram nele. Em determinado momento, gravamos a seguinte linha com o estado da atividade do usuário:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Em algum momento mais tarde, registramos a mudança na atividade do usuário e a gravamos nas duas linhas a seguir.

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

A primeira linha cancela o estado anterior do objeto (usuário). Ela deve copiar todos os campos do estado cancelado, exceto `Sign`.

A segunda linha contém o estado atual.

Como precisamos apenas do último estado da atividade do usuário, as linhas

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

pode ser excluído, colapsando o estado inválido (antigo) do objeto. `VersionedCollapsingMergeTree` faz isso durante a mesclagem das partes de dados.

Para entender por que precisamos de duas linhas para cada alteração, consulte [Algoritmo](#table_engines-versionedcollapsingmergetree-algorithm).

**Notas sobre o uso**

1. O programa que grava os dados deve lembrar o estado de um objeto para poder cancelá-lo. A string &quot;Cancel&quot; deve conter cópias dos campos da chave primária, da versão da string &quot;state&quot; e o `Sign` oposto. Isso aumenta o tamanho inicial do armazenamento, mas permite gravar os dados rapidamente.
2. Arrays longos e crescentes em colunas reduzem a eficiência do motor devido à sobrecarga de gravação. Quanto mais simples forem os dados, melhor será a eficiência.
3. Os resultados de `SELECT` dependem fortemente da consistência do histórico de alterações do objeto. Seja preciso ao preparar os dados para inserção. Dados inconsistentes podem produzir resultados imprevisíveis, como valores negativos para métricas não negativas, como a profundidade da sessão.

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### Algoritmo
</div>

Quando o ClickHouse mescla partes de dados, ele exclui cada par de linhas que têm a mesma chave primária e versão, mas `Sign` diferente. A ordem das linhas não importa.

Quando o ClickHouse insere dados, ele ordena as linhas pela chave primária. Se a coluna `Version` não estiver na chave primária, o ClickHouse a adiciona implicitamente à chave primária como o último campo e a usa para ordenação.

<div id="selecting-data">
  ## Seleção de dados
</div>

O ClickHouse não garante que todas as linhas com a mesma chave primária fiquem na mesma parte de dados resultante nem sequer no mesmo servidor físico. Isso vale tanto para a gravação dos dados quanto para a mesclagem subsequente das partes de dados. Além disso, o ClickHouse processa consultas `SELECT` com múltiplas threads e não pode prever a ordem das linhas no resultado. Isso significa que a agregação é necessária quando for preciso obter dados completamente &quot;colapsados&quot; de uma tabela `VersionedCollapsingMergeTree`.

Para concluir o collapsing, escreva uma consulta com uma cláusula `GROUP BY` e funções de agregação que levem em conta o sinal. Por exemplo, para calcular a quantidade, use `sum(Sign)` em vez de `count()`. Para calcular a soma de algum valor, use `sum(Sign * x)` em vez de `sum(x)` e adicione `HAVING sum(Sign) > 0`.

As funções de agregação `count`, `sum` e `avg` podem ser calculadas dessa forma. A função de agregação `uniq` pode ser calculada se um objeto tiver pelo menos um estado não colapsado. As funções de agregação `min` e `max` não podem ser calculadas porque o `VersionedCollapsingMergeTree` não armazena o histórico dos valores dos estados colapsados.

Se você precisar extrair os dados com &quot;collapsing&quot;, mas sem agregação (por exemplo, para verificar se há linhas cujos valores mais recentes correspondem a determinadas condições), pode usar o modificador `FINAL` para a cláusula `FROM`. Essa abordagem é ineficiente e não deve ser usada com tabelas grandes.

<div id="example-of-use">
  ## Exemplo de uso
</div>

Dados de exemplo:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Criando a tabela:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Inserindo os dados:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Usamos duas consultas `INSERT` para criar duas partes de dados diferentes. Se inserirmos os dados com uma única consulta, o ClickHouse criará uma parte de dados e nunca executará nenhum merge.

Obtendo os dados:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

O que vemos aqui e onde estão as partes colapsadas?
Criamos duas partes de dados usando duas consultas `INSERT`. A consulta `SELECT` foi executada em duas threads, e o resultado é uma ordem aleatória das linhas.
O colapsamento não ocorreu porque as partes de dados ainda não foram mescladas. O ClickHouse mescla partes de dados em algum momento imprevisível, que não podemos prever.

É por isso que precisamos de agregação:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Se não precisarmos de agregação e quisermos forçar o colapsamento, podemos usar o modificador `FINAL` na cláusula `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Esta é uma maneira muito ineficiente de selecionar dados. Não a use em tabelas grandes.