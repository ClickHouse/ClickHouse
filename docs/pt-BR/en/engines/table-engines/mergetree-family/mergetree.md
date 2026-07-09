---
description: 'Os motores de tabela da família `MergeTree` são projetados para altas taxas de ingestão de dados
  e grandes volumes de dados.'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'Motor de tabela MergeTree'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # Motor de tabela MergeTree
</div>

O motor `MergeTree` e outros motores da família `MergeTree` (por exemplo, `ReplacingMergeTree`, `AggregatingMergeTree`) são os motores de tabela mais usados e mais robustos no ClickHouse.

Os motores de tabela da família `MergeTree` foram projetados para altas taxas de ingestão de dados e volumes massivos de dados.
As operações de inserção criam partes de tabela, que são mescladas em segundo plano com outras partes de tabela.

Principais recursos dos motores de tabela da família `MergeTree`.

* A chave primária da tabela determina a ordem de ordenação dentro de cada parte de tabela (índice clusterizado). A chave primária também não faz referência a linhas individuais, mas a blocos de 8192 linhas chamados grânulos. Isso torna as chaves primárias de grandes conjuntos de dados pequenas o suficiente para permanecerem carregadas na memória principal, ao mesmo tempo em que ainda fornecem acesso rápido aos dados em disco.

* As tabelas podem ser particionadas usando uma expressão de partição arbitrária. O partition pruning garante que as partições sejam omitidas da leitura quando a consulta permitir.

* Os dados podem ser replicados em vários nós do cluster para alta disponibilidade, failover e upgrades sem indisponibilidade. Consulte [Replicação de dados](/pt-BR/engines/table-engines/mergetree-family/replication.md).

* Os motores de tabela `MergeTree` oferecem suporte a vários tipos de estatísticas e métodos de amostragem para ajudar na otimização de consultas.

:::note
Apesar do nome semelhante, o motor [Merge](/pt-BR/engines/table-engines/special/merge) é diferente dos motores `*MergeTree`.
:::

<div id="table_engine-mergetree-creating-a-table">
  ## Criando tabelas
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

Para ver uma descrição detalhada dos parâmetros, consulte a instrução [CREATE TABLE](/pt-BR/sql-reference/statements/create/table.md)

<div id="mergetree-query-clauses">
  ### Cláusulas de consulta
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — Nome e parâmetros do mecanismo. `ENGINE = MergeTree()`. O mecanismo `MergeTree` não tem parâmetros.

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — A chave de ordenação.

Uma Tuple com nomes de colunas ou expressões arbitrárias. Exemplo: `ORDER BY (CounterID + 1, EventDate)`.

Se nenhuma chave primária for definida (ou seja, `PRIMARY KEY` não tiver sido especificada), o ClickHouse usa a chave de ordenação como chave primária.

Se nenhuma ordenação for necessária, você pode usar a sintaxe `ORDER BY tuple()`.
Como alternativa, se a configuração `create_table_empty_primary_key_by_default` estiver habilitada, `ORDER BY ()` é adicionado implicitamente às instruções `CREATE TABLE`. Consulte [Selecionando uma chave primária](#selecting-a-primary-key).

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — A [chave de particionamento](/pt-BR/engines/table-engines/mergetree-family/custom-partitioning-key.md). Opcional. Na maioria dos casos, você não precisa de uma chave de particionamento e, se precisar particionar, em geral não precisará de uma chave de particionamento mais granular do que mensal. O particionamento não acelera as consultas (ao contrário da expressão ORDER BY). Você nunca deve usar um particionamento granular demais. Não particione seus dados por identificadores ou nomes de cliente (em vez disso, torne o identificador ou nome do cliente a primeira coluna na expressão ORDER BY).

Para particionar por mês, use a expressão `toYYYYMM(date_column)`, em que `date_column` é uma coluna com uma data do tipo [Date](/pt-BR/sql-reference/data-types/date.md). Os nomes das partições aqui têm o formato `"YYYYMM"`.

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — A chave primária, se ela [for diferente da chave de ordenação](#choosing-a-primary-key-that-differs-from-the-sorting-key). Opcional.

Especificar uma chave de ordenação (usando a cláusula `ORDER BY`) implica especificar uma chave primária.
Em geral, não é necessário especificar a chave primária além da chave de ordenação.

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — Uma expressão de amostragem. Opcional.

Se for especificada, deve estar contida na chave primária.
A expressão de amostragem deve resultar em um inteiro sem sinal.

Exemplo: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

<div id="ttl">
  #### TTL
</div>

`TTL` — Uma lista de regras que especifica a duração de armazenamento das linhas e a lógica de movimentação automática de partes [entre disks e volumes](#table_engine-mergetree-multiple-volumes). Opcional.

A expressão deve resultar em um `Date` ou `DateTime`, por exemplo, `TTL date + INTERVAL 1 DAY`.

O tipo da regra `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` especifica a ação a ser executada com a parte se a expressão for atendida (atingir o momento atual): remoção de linhas expiradas, movimentação de uma parte (se a expressão for atendida para todas as linhas em uma parte) para o disk especificado (`TO DISK 'xxx'`) ou para o volume (`TO VOLUME 'xxx'`), ou agregação de valores em linhas expiradas. O tipo padrão da regra é remoção (`DELETE`). É possível especificar uma lista com várias regras, mas não deve haver mais de uma regra `DELETE`.

Para mais detalhes, consulte [TTL para colunas e tabelas](#table_engine-mergetree-ttl)

<div id="settings">
  #### CONFIGURAÇÕES
</div>

Consulte [Configurações do MergeTree](../../../operations/settings/merge-tree-settings.md).

**Exemplo da configuração Sections**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

No exemplo, definimos o particionamento por mês.

Também definimos uma expressão de amostragem como um hash do ID do usuário. Isso permite pseudoaleatorizar os dados na tabela para cada `CounterID` e `EventDate`. Se você definir uma cláusula [SAMPLE](/pt-BR/sql-reference/statements/select/sample) ao selecionar os dados, o ClickHouse retornará uma amostra de dados pseudoaleatória e uniforme para um subconjunto de usuários.

A configuração `index_granularity` pode ser omitida porque 8192 é o valor padrão.

<details markdown="1">
  <summary>Método descontinuado para criar uma tabela</summary>

  :::note
  Não use este método em novos projetos. Se possível, migre os projetos antigos para o método descrito acima.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **Parâmetros do MergeTree()**

  * `date-column` — O nome de uma coluna do tipo [Date](/pt-BR/sql-reference/data-types/date.md). O ClickHouse cria automaticamente partições por mês com base nessa coluna. Os nomes das partições ficam no formato `"YYYYMM"`.
  * `sampling_expression` — Uma expressão de amostragem.
  * `(primary, key)` — Chave primária. Tipo: [Tuple()](/pt-BR/sql-reference/data-types/tuple.md)
  * `index_granularity` — A granularidade de um índice. O número de linhas de dados entre as &quot;marcas&quot; de um índice. O valor 8192 é adequado para a maioria das tarefas.

  **Exemplo**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  O mecanismo `MergeTree` é configurado da mesma forma que no exemplo acima, no método principal de configuração do mecanismo.
</details>

<div id="mergetree-data-storage">
  ## Armazenamento de dados
</div>

Uma tabela consiste em partes de dados ordenadas pela chave primária.

Quando dados são inseridos em uma tabela, são criadas partes de dados separadas, e cada uma delas é ordenada lexicograficamente pela chave primária. Por exemplo, se a chave primária for `(CounterID, Date)`, os dados na parte serão ordenados por `CounterID` e, dentro de cada `CounterID`, por `Date`.

Os dados pertencentes a partições diferentes são separados em partes diferentes. Em segundo plano, o ClickHouse mescla partes de dados para tornar o armazenamento mais eficiente. Partes que pertencem a partições diferentes não são mescladas. O mecanismo de mesclagem não garante que todas as linhas com a mesma chave primária fiquem na mesma parte de dados.

As partes de dados podem ser armazenadas no formato `Wide` ou `Compact`. No formato `Wide`, cada coluna é armazenada em um arquivo separado em um sistema de arquivos; no formato `Compact`, todas as colunas são armazenadas em um único arquivo. O formato `Compact` pode ser usado para aumentar o desempenho de inserções pequenas e frequentes.

O formato de armazenamento dos dados é controlado pelas configurações `min_bytes_for_wide_part` e `min_rows_for_wide_part` do motor de tabela. Se o número de bytes ou de linhas em uma parte de dados for menor que o valor da configuração correspondente, a parte será armazenada no formato `Compact`. Caso contrário, será armazenada no formato `Wide`. Se nenhuma dessas configurações estiver definida, as partes de dados serão armazenadas no formato `Wide`.

Cada parte de dados é dividida logicamente em grânulos. Um grânulo é o menor conjunto de dados indivisível que o ClickHouse lê ao selecionar dados. O ClickHouse não divide linhas nem valores, portanto cada grânulo sempre contém um número inteiro de linhas. A primeira linha de um grânulo é marcada com o valor da chave primária dessa linha. Para cada parte de dados, o ClickHouse cria um arquivo de índice que armazena as marcas. Para cada coluna, esteja ela na chave primária ou não, o ClickHouse também armazena as mesmas marcas. Essas marcas permitem localizar dados diretamente nos arquivos de coluna.

O tamanho do grânulo é limitado pelas configurações `index_granularity` e `index_granularity_bytes` do motor de tabela. O número de linhas em um grânulo fica no intervalo `[1, index_granularity]`, dependendo do tamanho das linhas. O tamanho de um grânulo pode exceder `index_granularity_bytes` se o tamanho de uma única linha for maior que o valor da configuração. Nesse caso, o tamanho do grânulo será igual ao tamanho da linha.

<div id="primary-keys-and-indexes-in-queries">
  ## Chaves primárias e índices em consultas
</div>

Considere a chave primária `(CounterID, Date)` como exemplo. Nesse caso, a ordenação e o índice podem ser ilustrados da seguinte forma:

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

Se a consulta especificar:

* `CounterID in ('a', 'h')`, o servidor lê os dados nos intervalos de marcas `[0, 3)` e `[6, 8)`.
* `CounterID IN ('a', 'h') AND Date = 3`, o servidor lê os dados nos intervalos de marcas `[1, 3)` e `[7, 8)`.
* `Date = 3`, o servidor lê os dados no intervalo de marcas `[1, 10]`.

Os exemplos acima mostram que usar um índice é sempre mais eficaz do que fazer uma varredura completa.

Um índice esparso permite a leitura de dados adicionais. Ao ler um único intervalo da chave primária, até `index_granularity * 2` linhas adicionais em cada bloco de dados podem ser lidas.

Índices esparsos permitem trabalhar com um número muito grande de linhas da tabela porque, na maioria dos casos, esses índices cabem na RAM do computador.

O ClickHouse não exige uma chave primária única. Você pode inserir várias linhas com a mesma chave primária.

É possível usar expressões do tipo `Nullable` nas cláusulas `PRIMARY KEY` e `ORDER BY`, mas isso é fortemente desaconselhado. Para permitir esse recurso, ative a configuração [allow&#95;nullable&#95;key](/pt-BR/operations/settings/merge-tree-settings/#allow_nullable_key). O princípio [NULLS&#95;LAST](/pt-BR/sql-reference/statements/select/order-by.md/#sorting-of-special-values) se aplica aos valores `NULL` na cláusula `ORDER BY`.

<div id="selecting-a-primary-key">
  ### Selecionando uma chave primária
</div>

O número de colunas na chave primária não tem um limite explícito. Dependendo da estrutura dos dados, você pode incluir mais ou menos colunas na chave primária. Isso pode:

* Melhorar o desempenho de um índice.

  Se a chave primária for `(a, b)`, adicionar outra coluna `c` melhorará o desempenho se as seguintes condições forem atendidas:

  * Houver consultas com uma condição na coluna `c`.
  * Forem comuns intervalos longos de dados (várias vezes maiores que a `index_granularity`) com valores idênticos para `(a, b)`. Em outras palavras, quando adicionar outra coluna permite ignorar intervalos de dados bem longos.

* Melhorar a compressão dos dados.

  O ClickHouse ordena os dados pela chave primária, portanto, quanto maior a consistência, melhor a compressão.

* Fornecer lógica adicional ao mesclar partes de dados nos motores [CollapsingMergeTree](/pt-BR/engines/table-engines/mergetree-family/collapsingmergetree) e [SummingMergeTree](/pt-BR/engines/table-engines/mergetree-family/summingmergetree.md).

  Nesse caso, faz sentido especificar uma *chave de ordenação* diferente da chave primária.

Uma chave primária longa afetará negativamente o desempenho de inserção e o consumo de memória, mas colunas extras na chave primária não afetam o desempenho do ClickHouse durante consultas `SELECT`.

Você pode criar uma tabela sem chave primária usando a sintaxe `ORDER BY tuple()`. Nesse caso, o ClickHouse armazena os dados na ordem de inserção. Se quiser preservar a ordem dos dados ao inseri-los com consultas `INSERT ... SELECT`, defina [max&#95;insert&#95;threads = 1](/pt-BR/operations/settings/settings#max_insert_threads).

Para selecionar os dados na ordem original, use consultas `SELECT` [de thread única](/pt-BR/operations/settings/settings.md/#max_threads).

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### Escolhendo uma chave primária diferente da chave de ordenação
</div>

É possível especificar uma chave primária (uma expressão com valores gravados no arquivo de índice para cada marca) diferente da chave de ordenação (uma expressão usada para ordenar as linhas nas partes de dados). Nesse caso, a tupla da expressão da chave primária deve ser um prefixo da tupla da expressão da chave de ordenação.

Esse recurso é útil ao usar os motores de tabela [SummingMergeTree](/pt-BR/engines/table-engines/mergetree-family/summingmergetree.md) e
[AggregatingMergeTree](/pt-BR/engines/table-engines/mergetree-family/aggregatingmergetree.md). Em um cenário comum de uso desses motores, a tabela tem dois tipos de colunas: *dimensões* e *medidas*. Consultas típicas agregam valores das colunas de medida com `GROUP BY` arbitrário e filtragem por dimensões. Como SummingMergeTree e AggregatingMergeTree agregam linhas com o mesmo valor da chave de ordenação, é natural adicionar todas as dimensões a ela. Como resultado, a expressão de chave passa a consistir em uma longa lista de colunas, e essa lista precisa ser atualizada com frequência à medida que novas dimensões são adicionadas.

Nesse caso, faz sentido manter apenas algumas colunas na chave primária, para fornecer varreduras por intervalo eficientes, e adicionar as colunas de dimensão restantes à tupla da chave de ordenação.

O [ALTER](/pt-BR/sql-reference/statements/alter/index.md) da chave de ordenação é uma operação leve porque, quando uma nova coluna é adicionada simultaneamente à tabela e à chave de ordenação, as partes de dados existentes não precisam ser alteradas. Como a chave de ordenação antiga é um prefixo da nova chave de ordenação e não há dados na coluna recém-adicionada, os dados ficam ordenados tanto pela chave de ordenação antiga quanto pela nova no momento da modificação da tabela.

<div id="use-of-indexes-and-partitions-in-queries">
  ### Uso de índices e partições em consultas
</div>

Para consultas `SELECT`, o ClickHouse analisa se um índice pode ser usado. Um índice pode ser usado se a cláusula `WHERE/PREWHERE` contiver uma expressão (como um dos elementos da conjunção, ou por completo) que represente uma operação de comparação de igualdade ou desigualdade, ou se contiver `IN` ou `LIKE` com um prefixo fixo em colunas ou expressões que façam parte da chave primária ou da chave de particionamento, ou em determinadas funções parcialmente repetitivas dessas colunas, ou ainda em relações lógicas entre essas expressões.

Assim, é possível executar consultas rapidamente em um ou vários intervalos da chave primária. Neste exemplo, as consultas serão rápidas quando executadas para uma tag de rastreamento específica, para uma tag específica e um intervalo de datas, para uma tag específica e uma data, para várias tags com um intervalo de datas, e assim por diante.

Vejamos a engine configurada da seguinte forma:

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

Nesse caso, nas consultas:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

O ClickHouse usará o índice da chave primária para descartar dados desnecessários e a chave de particionamento mensal para descartar partições que estejam fora dos intervalos de datas adequados.

As consultas acima mostram que o índice é usado mesmo para expressões complexas. A leitura da tabela é organizada de modo que usar o índice não pode ser mais lento do que uma varredura completa.

No exemplo abaixo, o índice não pode ser usado.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Para verificar se o ClickHouse pode usar o índice ao executar uma consulta, use as configurações [force&#95;index&#95;by&#95;date](/pt-BR/operations/settings/settings.md/#force_index_by_date) e [force&#95;primary&#95;key](/pt-BR/operations/settings/settings#force_primary_key).

A chave de particionamento por mês permite ler apenas os blocos de dados que contêm datas no intervalo correto. Nesse caso, o bloco de dados pode conter dados de muitas datas (até um mês inteiro). Dentro de um bloco, os dados são ordenados pela chave primária, que pode não ter a data como primeira coluna. Por isso, usar uma consulta com apenas uma condição de data, sem especificar o prefixo da chave primária, fará com que mais dados sejam lidos do que no caso de uma única data.

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### Uso do índice para expressões determinísticas em chaves primárias
</div>

A chave primária pode conter expressões, não apenas nomes de colunas. Essas expressões não se limitam a cadeias simples de funções: elas podem ser árvores de expressões arbitrárias (por exemplo, funções aninhadas e expressões compostas), desde que sejam determinísticas.

Uma expressão é **determinística** se sempre retorna o mesmo resultado para os mesmos valores de entrada (por exemplo: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`; diferentemente de `now()` ou `rand()`). Se a chave primária contiver expressões determinísticas, o ClickHouse poderá aplicá-las a valores constantes da consulta e usar o resultado para criar condições no índice da chave primária. Isso permite pular dados para predicados como `=`, `IN` e `has`.

Um caso de uso comum é manter a chave primária compacta (por exemplo, armazenar um hash em vez de uma `String` longa), sem deixar de permitir que predicados sobre a coluna original usem o índice.

Exemplo de uma chave primária determinística (mas não injetiva):

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

Exemplos de predicados que podem usar o índice:

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

Nesses casos, o ClickHouse calcula `length('alice')` (e outras constantes) uma única vez e usa os valores de comprimento para restringir os intervalos no índice da chave primária. Como o comprimento de uma string **não é injetivo**, diferentes strings `user_id` podem ter o mesmo comprimento, então o índice pode ler grânulos extras (falsos positivos). O resultado continua correto porque o predicado original (`user_id = ...`, `IN` etc.) ainda é aplicado após a leitura.

Se a expressão determinística também for **injetiva** (entradas diferentes não podem produzir a mesma saída para os tipos de argumento usados), o ClickHouse também pode usar o índice de forma eficaz para as formas negadas: `!=`, `NOT IN` e `NOT has(...)`. Por exemplo, `reverse(p)` e `hex(p)` são injetivas para `String`.

Exemplo de uma chave primária injetiva:

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

Expressões injetivas mais complexas também são suportadas, por exemplo:

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

Exemplos de predicados que podem usar o índice:

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### Uso do índice para chaves primárias parcialmente monotônicas
</div>

Considere, por exemplo, os dias do mês. Eles formam uma [sequência monotônica](https://en.wikipedia.org/wiki/Monotonic_function) ao longo de um mês, mas deixam de ser monotônicos em períodos mais longos. Essa é uma sequência parcialmente monotônica. Se um usuário criar a tabela com uma chave primária parcialmente monotônica, o ClickHouse criará um índice esparso, como de costume. Quando um usuário consulta dados desse tipo de tabela, o ClickHouse analisa as condições da consulta. Se o usuário quiser obter dados entre duas marcas do índice, e ambas estiverem dentro do mesmo mês, o ClickHouse poderá usar o índice nesse caso específico, porque consegue calcular a distância entre os parâmetros de uma consulta e as marcas do índice.

O ClickHouse não pode usar o índice se os valores da chave primária no intervalo de parâmetros da consulta não representarem uma sequência monotônica. Nesse caso, o ClickHouse usa o método de varredura completa.

O ClickHouse usa essa lógica não apenas para sequências de dias do mês, mas para qualquer chave primária que represente uma sequência parcialmente monotônica.

<div id="table_engine-mergetree-data_skipping-indexes">
  ### Data skipping indexes
</div>

A declaração do índice fica na seção de colunas da consulta `CREATE`.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

Para tabelas da família `*MergeTree`, é possível especificar índices de salto de dados.

Esses índices agregam informações sobre a expressão especificada em blocos, que consistem em `granularity_value` grânulos (o tamanho do grânulo é especificado pela configuração `index_granularity` no motor de tabela). Em seguida, essas agregações são usadas em consultas `SELECT` para reduzir a quantidade de dados lidos do disco, pulando grandes blocos de dados nos quais a consulta `where` não pode ser satisfeita.

A cláusula `GRANULARITY` pode ser omitida; o valor padrão de `granularity_value` é 1.

**Exemplo**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Os índices do exemplo podem ser usados pelo ClickHouse para reduzir a quantidade de dados lidos do disco nas consultas abaixo:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

Data skipping indexes também podem ser criados em colunas compostas:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### Tipos de skip indexes
</div>

O motor de tabela `MergeTree` oferece suporte aos seguintes tipos de skip indexes.
Para mais informações sobre como os skip indexes podem ser usados para otimizar o desempenho,
consulte [&quot;Entendendo os data skipping indexes do ClickHouse&quot;](/pt-BR/optimize/skipping-indexes).

* índice [`MinMax`](#minmax)
* índice [`Set`](#set)
* índice [`bloom_filter`](#bloom-filter)
* índice [`ngrambf_v1`](#n-gram-bloom-filter) *(Obsoleto)*
* índice [`tokenbf_v1`](#token-bloom-filter) *(Obsoleto)*
* índice [`text`](#text)
* índice [`vector_similarity`](#vector-similarity)

<div id="minmax">
  #### Índice de salto MinMax
</div>

Para cada grânulo de índice, são armazenados os valores mínimo e máximo de uma expressão.
(Se a expressão for do tipo `tuple`, serão armazenados os valores mínimo e máximo de cada elemento da tupla.)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

Em cada grânulo de índice, são armazenados no máximo `max_rows` valores únicos da expressão especificada.
`max_rows = 0` significa &quot;armazenar todos os valores únicos&quot;.

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### Filtro de Bloom
</div>

Armazena um [filtro de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) para as colunas especificadas em cada grânulo de índice.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

O parâmetro `false_positive_rate` pode assumir um valor entre 0 e 1 (por padrão: `0.025`) e especifica a probabilidade de gerar um resultado positivo (o que aumenta a quantidade de dados a ser lida).

Os seguintes tipos de dados são suportados:

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Tipo de dados `Map`: especificando a criação de índice com chaves ou valores
Para o tipo de dados `Map`, o cliente pode especificar se o índice deve ser criado para chaves ou valores usando as funções [`mapKeys`](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapKeys) ou [`mapValues`](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapValues).
:::

:::note Tipo de dados JSON: indexação de caminhos JSON
Para o tipo de dados [`JSON`](/pt-BR/sql-reference/data-types/newjson), um índice de filtro de Bloom pode ser criado no conjunto de caminhos usando a função [`JSONAllPaths`](/pt-BR/sql-reference/functions/json-functions#JSONAllPaths). Isso permite ignorar grânulos nos quais um caminho JSON consultado está ausente. Consulte [Data skipping indexes for JSON](/pt-BR/sql-reference/data-types/newjson#data-skipping-indexes-for-json) para mais detalhes.
:::

<div id="n-gram-bloom-filter">
  #### Filtro de Bloom de n-gramas *(Descontinuado)*
</div>

:::note
Com a disponibilidade geral (GA) do índice `text` a partir da versão 26.2 do ClickHouse, o índice `ngrambf_v1` não é mais recomendado para pesquisa de texto completo.

Consulte a página [&quot;Pesquisa de texto completo com índices de texto&quot;](./textindexes.md) para mais detalhes.
:::

Para cada grânulo de índice, é armazenado um [filtro de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) para os [n-gramas](https://en.wikipedia.org/wiki/N-gram) das colunas especificadas.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| Parâmetro                       | Descrição                                                                                                                                 |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `n`                             | tamanho do ngram                                                                                                                          |
| `size_of_bloom_filter_in_bytes` | Tamanho do filtro de Bloom em bytes. Você pode usar um valor alto aqui, por exemplo, `256` ou `512`, porque ele pode ser bem comprimido). |
| `number_of_hash_functions`      | Número de funções hash usadas no filtro de Bloom.                                                                                         |
| `random_seed`                   | Semente para as funções hash do filtro de Bloom.                                                                                          |

Este índice funciona apenas com os seguintes tipos de dados:

* [`String`](/pt-BR/sql-reference/data-types/string.md)
* [`FixedString`](/pt-BR/sql-reference/data-types/fixedstring.md)
* [`Map`](/pt-BR/sql-reference/data-types/map.md)

Para estimar os parâmetros de `ngrambf_v1`, você pode usar as seguintes [funções definidas pelo usuário (UDFs)](/pt-BR/sql-reference/statements/create/function.md).

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

Para usar essas funções, você precisa especificar pelo menos dois parâmetros:

* `total_number_of_all_grams`
* `probability_of_false_positives`

Por exemplo, há `4300` ngrams no granule e você espera que a taxa de falsos positivos seja inferior a `0.0001`.
Os outros parâmetros podem então ser estimados executando as seguintes consultas:

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

Claro, você também pode usar essas funções para estimar parâmetros para outras situações.
As funções acima fazem referência à calculadora de filtro de Bloom [aqui](https://hur.st/bloomfilter).

<div id="token-bloom-filter">
  #### Filtro de Bloom para tokens
</div>

:::note
Com a disponibilidade geral (GA) do índice `text` a partir da versão 26.2 do ClickHouse, o índice `tokenbf_v1` não é mais recomendado para pesquisa em texto completo.

Consulte a página [&quot;Pesquisa em texto completo com índices de texto&quot;](./textindexes.md) para mais detalhes.
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### Filtro de Bloom de gramas esparsos
</div>

O filtro de Bloom de gramas esparsos é semelhante ao `ngrambf_v1`, mas usa [tokens de gramas esparsos](/pt-BR/sql-reference/functions/string-functions.md/#sparseGrams) em vez de ngrams.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### Índice de texto
</div>

Cria um índice invertido sobre dados textuais tokenizados, permitindo uma pesquisa de texto completo eficiente e determinística. Consulte [aqui](textindexes.md) mais detalhes.

<div id="vector-similarity">
  #### Similaridade vetorial
</div>

Oferece suporte à busca aproximada por vizinhos mais próximos; consulte [aqui](annindexes.md) para mais detalhes.

<div id="functions-support">
  ### Suporte a funções
</div>

As condições na cláusula `WHERE` contêm chamadas a funções que operam sobre colunas. Se a coluna fizer parte de um índice, o ClickHouse tenta usar esse índice ao executar essas funções. O ClickHouse oferece suporte a diferentes subconjuntos de funções compatíveis com o uso de índices.

Índices do tipo `set` podem ser utilizados por todas as funções. Os outros tipos de índice têm suporte da seguinte forma:

| Função (operador) / índice                                                                                                | chave primária | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | texto |
| ------------------------------------------------------------------------------------------------------------------------- | -------------- | ------ | -------------- | -------------- | ---------------- | ---------------- | ----- |
| [equals (=, ==)](/pt-BR/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [notEquals(!=, &lt;&gt;)](/pt-BR/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [like](/pt-BR/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [notLike](/pt-BR/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✗     |
| [match](/pt-BR/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗              | ✗      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [startsWith](/pt-BR/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [endsWith](/pt-BR/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗              | ✗      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [multiSearchAny](/pt-BR/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗              | ✗      | ✔              | ✗              | ✗                | ✗                | ✔     |
| [multiSearchAnyUTF8](/pt-BR/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [multiMatchAny](/pt-BR/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [in](/pt-BR/sql-reference/functions/in-functions)                                                                               | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [notIn](/pt-BR/sql-reference/functions/in-functions)                                                                            | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [less (`<`)](/pt-BR/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [greater (`>`)](/pt-BR/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [lessOrEquals (`<=`)](/pt-BR/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [greaterOrEquals (`>=`)](/pt-BR/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [empty](/pt-BR/sql-reference/functions/array-functions/#empty)                                                                  | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [notEmpty](/pt-BR/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗              | ✔      | ✗              | ✗              | ✗                | ✔                | ✗     |
| [has](/pt-BR/sql-reference/functions/array-functions#has)                                                                       | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [hasAny](/pt-BR/sql-reference/functions/array-functions#hasAny)                                                                 | ✗              | ✗      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [hasAll](/pt-BR/sql-reference/functions/array-functions#hasAll)                                                                 | ✗              | ✗      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [hasToken](/pt-BR/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✔     |
| [hasTokenOrNull](/pt-BR/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✔     |
| [hasTokenCaseInsensitive (`*`)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✗     |
| [hasTokenCaseInsensitiveOrNull (`*`)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✗     |
| [hasAnyTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [hasAllTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [pointInPolygon](/pt-BR/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [mapContains (mapContainsKey)](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsKeyLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsValue](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsValueLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |

Funções com um argumento constante menor que o tamanho do ngram não podem ser usadas pelo `ngrambf_v1` para otimização de consultas.

(*) Para que `hasTokenCaseInsensitive` e `hasTokenCaseInsensitiveOrNull` sejam eficazes, o índice `tokenbf_v1` deve ser criado sobre dados em minúsculas, por exemplo `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
Filtros de Bloom podem gerar correspondências falso-positivas, portanto os índices `ngrambf_v1`, `tokenbf_v1`, `sparse_grams` e `bloom_filter` não podem ser usados para otimizar consultas em que se espera que o resultado de uma função seja falso.

Por exemplo:

* Pode ser otimizado:
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* Não pode ser otimizado:
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## Projeções
</div>

As projeções são como [visões materializadas](/pt-BR/sql-reference/statements/create/view), mas são definidas no nível das partes. Elas oferecem garantias de consistência, além de uso automático nas consultas.

:::note
Ao implementar projeções, você também deve considerar a configuração [force&#95;optimize&#95;projection](/pt-BR/operations/settings/settings#force_optimize_projection).
:::

As projeções não são compatíveis com instruções `SELECT` que usam o modificador [FINAL](/pt-BR/sql-reference/statements/select/from#final-modifier).

<div id="projection-query">
  ### Consulta de projeção
</div>

Uma consulta de projeção define uma projeção. Ela seleciona implicitamente dados da tabela pai.
**Sintaxe**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

As projeções podem ser modificadas ou excluídas com a instrução [ALTER](/pt-BR/sql-reference/statements/alter/projection.md).

<div id="projection-index">
  ### Índices de projeção
</div>

Os índices de projeção estendem o subsistema de projeções, oferecendo uma forma leve e explícita de definir índices no nível da projeção.
Externamente, um índice de projeção ainda é uma projeção, mas com sintaxe simplificada e propósito mais claro: define uma expressão dedicada à filtragem, em vez de servir dados materializados.
Internamente, um índice de projeção não materializa a tabela original em uma ordem permutada de linhas, como uma projeção comum.
Em vez disso, a permutação é armazenada na forma de uma coluna numérica de permutação `_part_offset`, ou seja, `SELECT _part_offset ORDER BY <index_expr>`.

<div id="projection-index-syntax">
  #### Sintaxe
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

Exemplo:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### Tipos de índice
</div>

Atualmente, há suporte para:

* **basic**: equivalente a um índice MergeTree normal na expressão.

O mecanismo permite adicionar mais tipos de índice no futuro.

<div id="projection-storage">
  ### Armazenamento de projeções
</div>

As projeções são armazenadas dentro do diretório da parte. É semelhante a um índice, mas contém um subdiretório que armazena a parte de uma tabela `MergeTree` anônima. A tabela é derivada da consulta de definição da projeção. Se houver uma cláusula `GROUP BY`, o mecanismo de armazenamento subjacente se torna [AggregatingMergeTree](aggregatingmergetree.md), e todas as funções de agregação são convertidas em `AggregateFunction`. Se houver uma cláusula `ORDER BY`, a tabela `MergeTree` a utiliza como expressão de chave primária. Durante o processo de merge, a parte da projeção é mesclada por meio da rotina de merge do seu armazenamento. O checksum da parte da tabela pai é combinado com o da parte da projeção. Outras tarefas de manutenção são semelhantes às dos skip indices.

<div id="projection-query-analysis">
  ### Análise da consulta
</div>

1. Verifique se a projeção pode ser usada para responder à consulta especificada, ou seja, se ela gera o mesmo resultado que uma consulta na tabela base.
2. Selecione a melhor correspondência viável, com a menor quantidade de grânulos a serem lidos.
3. O pipeline de consulta que usa projeções será diferente daquele que usa as partes originais. Se a projeção estiver ausente em algumas partes, podemos adicionar o pipeline para &quot;projetá-la&quot; em tempo real.

<div id="concurrent-data-access">
  ## Acesso concorrente a dados
</div>

Para acesso concorrente à tabela, usamos multiversionamento. Em outras palavras, quando uma tabela é lida e atualizada simultaneamente, os dados são lidos de um conjunto de partes atual no momento da consulta. Não há bloqueios prolongados. As inserções não interferem nas operações de leitura.

A leitura de uma tabela é paralelizada automaticamente.

<div id="table_engine-mergetree-ttl">
  ## TTL para colunas e tabelas
</div>

Determina o ciclo de vida dos valores.

A cláusula `TTL` pode ser definida para a tabela inteira e para cada coluna individual. O `TTL` no nível da tabela também pode especificar a lógica de movimentação automática de dados entre disks e volumes, ou de recompressão de partes em que todos os dados expiraram.

As expressões devem resultar no tipo de dado [Date](/pt-BR/sql-reference/data-types/date.md), [Date32](/pt-BR/sql-reference/data-types/date32.md), [DateTime](/pt-BR/sql-reference/data-types/datetime.md) ou [DateTime64](/pt-BR/sql-reference/data-types/datetime64.md).

:::tip[Evite funções não determinísticas em expressões TTL]
O TTL é avaliado durante mesclagens em segundo plano, e não no momento da inserção.
Funções como `rand()`, `now()`, ou `now64()` serão reavaliadas em cada merge, levando a um comportamento de exclusão imprevisível.
O ClickHouse bloqueia expressões sem qualquer dependência de coluna, mas atualmente não rejeita funções não determinísticas combinadas com uma referência de coluna (por exemplo, `ts + rand()`). As expressões TTL devem se basear exclusivamente em valores determinísticos derivados de colunas para garantir resultados previsíveis.
:::

**Sintaxe**

Definindo o time-to-live para uma coluna:

```sql
TTL time_column
TTL time_column + interval
```

Para definir `interval`, use os operadores de [intervalo de tempo](/pt-BR/sql-reference/operators#operators-for-working-with-dates-and-times), por exemplo:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### TTL de coluna
</div>

Quando os valores de uma coluna expiram, o ClickHouse os substitui pelos valores padrão do tipo de dado da coluna. Se todos os valores da coluna em uma parte de dados expirarem, o ClickHouse exclui essa coluna da parte de dados no sistema de arquivos.

A cláusula `TTL` não pode ser usada em colunas-chave.

**Exemplos**

<div id="creating-a-table-with-ttl">
  #### Criando uma tabela com `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### Adicionando TTL a uma coluna de uma tabela existente
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### Alterando o TTL da coluna
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### TTL da tabela
</div>

A tabela pode ter uma expressão para remover linhas expiradas e várias expressões para a movimentação automática de partes entre [disks ou volumes](#table_engine-mergetree-multiple-volumes). Quando as linhas da tabela expiram, o ClickHouse exclui todas as linhas correspondentes. Para a movimentação ou recompressão de partes, todas as linhas de uma parte devem atender aos critérios da expressão `TTL`.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

O tipo de regra TTL pode seguir cada expressão TTL. Ela determina a ação a ser executada quando a expressão for satisfeita (atingir o momento atual):

* `DELETE` - exclui linhas expiradas (ação padrão);
* `RECOMPRESS codec_name` - recomprime a parte de dados com `codec_name`;
* `TO DISK 'aaa'` - move a parte para o disco `aaa`;
* `TO VOLUME 'bbb'` - move a parte para o disco `bbb`;
* `GROUP BY` - agrega linhas expiradas.

A ação `DELETE` pode ser usada em conjunto com a cláusula `WHERE` para excluir apenas algumas das linhas expiradas com base em uma condição de filtragem:

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

A expressão `GROUP BY` deve ser um prefixo da chave primária da tabela.

Se uma coluna não fizer parte da expressão `GROUP BY` e não for definida explicitamente na cláusula `SET`, ela conterá, na linha de resultado, um valor arbitrário das linhas agrupadas (como se a função de agregação `any` fosse aplicada a ela).

**Exemplos**

<div id="creating-a-table-with-ttl">
  #### Criando uma tabela com `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### Alterando o `TTL` da tabela:
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

Criando uma tabela em que as linhas expiram após um mês. As linhas expiradas cujas datas caem em segundas-feiras são excluídas:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### Criando uma tabela em que as linhas expiradas são recomprimidas:
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

Criando uma tabela em que as linhas expiradas são agregadas. Nas linhas resultantes, `x` contém o valor máximo entre as linhas agrupadas, `y` — o valor mínimo, e `d` — algum valor arbitrário das linhas agrupadas.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### Removendo dados expirados
</div>

Dados com `TTL` expirado são removidos quando o ClickHouse faz merge das partes de dados.

Quando o ClickHouse detecta que os dados expiraram, ele executa um merge fora do agendamento. Para controlar a frequência desses merges, você pode definir `merge_with_ttl_timeout`. Se o valor for muito baixo, muitos merges fora do agendamento serão executados, o que pode consumir muitos recursos.

Se você executar a consulta `SELECT` entre os merges, poderá obter dados expirados. Para evitar isso, use a consulta [OPTIMIZE](/pt-BR/sql-reference/statements/optimize.md) antes de `SELECT`.

**Veja também**

* configuração [ttl&#95;only&#95;drop&#95;parts](/pt-BR/operations/settings/merge-tree-settings#ttl_only_drop_parts)

<div id="disk-types">
  ## Tipos de disk
</div>

Além dos dispositivos de bloco locais, o ClickHouse oferece suporte a estes tipos de armazenamento:

* [`s3` para S3 e MinIO](#table_engine-mergetree-s3)
* [`gcs` para GCS](/pt-BR/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` para Azure Blob Storage](/pt-BR/operations/storing-data#azure-blob-storage)
* [`hdfs` para HDFS](/pt-BR/engines/table-engines/integrations/hdfs)
* [`web` para acesso via web somente leitura](/pt-BR/operations/storing-data#web-storage)
* [`cache` para cache local](/pt-BR/operations/storing-data#using-local-cache)
* [`s3_plain` para backups no S3](/pt-BR/operations/backup/disk)
* [`s3_plain_rewritable` para tabelas imutáveis não replicadas no S3](/pt-BR/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## Uso de vários dispositivos de bloco para armazenamento de dados
</div>

<div id="introduction">
  ### Introdução
</div>

Os motores de tabela da família `MergeTree` podem armazenar dados em vários dispositivos de bloco. Por exemplo, isso pode ser útil quando os dados de uma determinada tabela são implicitamente divididos em &quot;quentes&quot; e &quot;frios&quot;. Os dados mais recentes são consultados com frequência, mas exigem apenas uma pequena quantidade de espaço. Em contrapartida, o grande volume de dados históricos é consultado raramente. Se houver vários disks disponíveis, os dados &quot;quentes&quot; podem ficar em disks rápidos (por exemplo, SSDs NVMe ou na memória), enquanto os dados &quot;frios&quot; ficam em disks relativamente lentos (por exemplo, HDD).

Isso se aplica a todos os tipos de disk, incluindo S3 e outros disks de armazenamento de objetos. Por exemplo, você pode distribuir dados entre vários buckets do S3 em um único volume ou criar políticas em camadas que movem dados de disks locais para o S3. Consulte [Using S3 disks with multiple volumes](#s3-multiple-volumes) para mais detalhes.

Uma parte de dados é a menor unidade móvel para tabelas com o motor `MergeTree`. Os dados pertencentes a uma parte são armazenados em um disk. As partes de dados podem ser movidas entre disks em segundo plano (de acordo com a configuração do usuário), bem como por meio das consultas [ALTER](/pt-BR/sql-reference/statements/alter/partition).

<div id="terms">
  ### Termos
</div>

* Disk — Dispositivo de bloco montado no sistema de arquivos.
* Default disk — Disk que armazena o caminho especificado na configuração de servidor [path](/pt-BR/operations/server-configuration-parameters/settings.md/#path).
* Volume — Conjunto ordenado de disks equivalentes (semelhante a [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
* Storage policy — Conjunto de volumes e regras para mover dados entre eles.

Os nomes atribuídos às entidades descritas podem ser encontrados nas tabelas de sistema [system.storage&#95;policies](/pt-BR/operations/system-tables/storage_policies) e [system.disks](/pt-BR/operations/system-tables/disks). Para aplicar uma das políticas de armazenamento configuradas a uma tabela, use a configuração `storage_policy` das tabelas da família de engine `MergeTree`.

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### Configuração
</div>

Disks, volumes e políticas de armazenamento devem ser declarados dentro da tag `<storage_configuration>` ou em um arquivo no diretório `config.d`.

:::tip
Os disks também podem ser declarados na seção `SETTINGS` de uma consulta. Isso é útil
para análises ad hoc, permitindo adicionar temporariamente um disk que esteja, por exemplo, disponível em uma URL.
Consulte [armazenamento dinâmico](/pt-BR/operations/storing-data#dynamic-configuration) para mais detalhes.
:::

Estrutura da configuração:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Tags:

* `<disk_name_N>` — Nome do disk. Os nomes devem ser diferentes para todos os disks.
* `path` — caminho em que o servidor armazenará os dados (pastas `data` e `shadow`); deve terminar com &#39;/&#39;.
* `keep_free_space_bytes` — quantidade de espaço livre em disk a ser reservada.

A ordem da definição do disk não é importante.

Estrutura de configuração das políticas de armazenamento:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Tags:

* `policy_name_N` — Nome da política. Os nomes de política devem ser exclusivos.
* `volume_name_N` — Nome do volume. Os nomes de volume devem ser exclusivos.
* `disk` — um disk dentro de um volume.
* `max_data_part_size_bytes` — o tamanho máximo de uma parte que pode ser armazenada em qualquer um dos disks do volume. Se o tamanho estimado de uma parte mesclada for maior que `max_data_part_size_bytes`, essa parte será gravada no próximo volume. Basicamente, esse recurso permite manter partes novas/pequenas em um volume hot (SSD) e movê-las para um volume cold (HDD) quando atingirem um tamanho maior. Não use essa configuração se a sua política tiver apenas um volume.
* `move_factor` — quando a quantidade de espaço disponível fica abaixo desse fator, os dados começam automaticamente a ser movidos para o próximo volume, se houver (por padrão, 0.1). O ClickHouse ordena as partes existentes por tamanho, da maior para a menor (em ordem decrescente), e seleciona partes cujo tamanho total seja suficiente para atender à condição de `move_factor`. Se o tamanho total de todas as partes for insuficiente, todas as partes serão movidas.
* `perform_ttl_move_on_insert` — Desabilita o TTL move no INSERT de parte de dados. Por padrão (se estiver habilitado), se inserirmos uma parte de dados que já expirou pela regra de TTL move, ela irá imediatamente para um volume/disk declarado na regra de movimentação. Isso pode deixar o insert significativamente mais lento caso o volume/disk de destino seja lento (por exemplo, S3). Se estiver desabilitado, a parte de dados já expirada será gravada em um volume padrão e, logo em seguida, movida para o volume de TTL.
* `load_balancing` - Política de balanceamento de disks, `round_robin` ou `least_used`.
* `least_used_ttl_ms` - Configura o timeout (em milissegundos) para atualizar o espaço disponível em todos os disks (`0` - sempre atualizar, `-1` - nunca atualizar, o padrão é `60000`). Observe que, se o disk puder ser usado apenas pelo ClickHouse e não estiver sujeito a redimensionamento/redução online do filesystem, você pode usar `-1`; em todos os outros casos, isso não é recomendado, pois eventualmente levará a uma distribuição incorreta do espaço.
* `prefer_not_to_merge` — Você não deve usar essa configuração. Desabilita a mesclagem de partes de dados neste volume (isso é prejudicial e leva à degradação de desempenho). Quando essa configuração está habilitada (não faça isso), a mesclagem de dados neste volume não é permitida (o que é ruim). Isso permite (mas você não precisa disso) controlar (se você quiser controlar alguma coisa, está cometendo um erro) como o ClickHouse trabalha com disks lentos (mas o ClickHouse sabe o que faz, então, por favor, não use essa configuração).
* `volume_priority` — Define a prioridade (ordem) em que os volumes são preenchidos. Um valor menor significa prioridade maior. Os valores do parâmetro devem ser números naturais e, em conjunto, cobrir o intervalo de 1 a N (com a menor prioridade atribuída), sem pular nenhum número.
  * Se *todos* os volumes estiverem marcados, eles serão priorizados na ordem fornecida.
  * Se apenas *alguns* volumes estiverem marcados, aqueles sem a marcação terão a menor prioridade e serão priorizados na ordem em que são definidos no config.
  * Se *nenhum* volume estiver marcado, sua prioridade será definida de acordo com a ordem em que for declarado na configuração.
  * Dois volumes não podem ter o mesmo valor de prioridade.

Exemplos de configuração:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

No exemplo apresentado, a política `hdd_in_order` implementa a abordagem de [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling). Assim, essa política define apenas um volume (`single`), e as partes de dados são armazenadas em todos os seus disks em ordem circular. Essa política pode ser bastante útil se houver vários disks semelhantes montados no sistema, mas sem RAID configurado. Tenha em mente que cada disk individualmente não é confiável, e talvez seja interessante compensar isso com um fator de replicação de 3 ou mais.

Se houver diferentes tipos de disks disponíveis no sistema, a política `moving_from_ssd_to_hdd` poderá ser usada no lugar. O volume `hot` consiste em um disk SSD (`fast_ssd`), e o tamanho máximo de uma parte que pode ser armazenada nesse volume é 1GB. Todas as partes com tamanho superior a 1GB serão armazenadas diretamente no volume `cold`, que contém o disk HDD `disk1`.
Além disso, quando a ocupação do disk `fast_ssd` ultrapassar 80%, os dados serão transferidos para `disk1` por um processo em segundo plano.

A ordem de enumeração dos volumes dentro de uma política de armazenamento é importante caso pelo menos um dos volumes listados não tenha um parâmetro `volume_priority` explícito.
Quando um volume fica cheio demais, os dados são movidos para o próximo. A ordem de enumeração dos disks também é importante, porque os dados são armazenados neles em rodízio.

Ao criar uma tabela, é possível aplicar a ela uma das políticas de armazenamento configuradas:

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

A política de armazenamento `default` pressupõe o uso de apenas um volume, que consiste em um único disk especificado em `<path>`.
Você pode alterar a política de armazenamento após a criação da tabela usando a consulta [ALTER TABLE ... MODIFY SETTING]; a nova política deve incluir todos os disks e volumes antigos com os mesmos nomes.

O número de threads que executam movimentações em segundo plano de partes de dados pode ser alterado pela configuração [background&#95;move&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings.md/#background_move_pool_size).

<div id="details">
  ### Detalhes
</div>

No caso das tabelas `MergeTree`, os dados chegam ao disk de diferentes maneiras:

* Como resultado de uma inserção (consulta `INSERT`).
* Durante merges em segundo plano e [mutações](/pt-BR/sql-reference/statements/alter#mutations).
* Durante o download a partir de outra réplica.
* Como resultado do congelamento de partição [ALTER TABLE ... FREEZE PARTITION](/pt-BR/sql-reference/statements/alter/partition#freeze-partition).

Em todos esses casos, exceto em mutações e no congelamento de partição, uma parte é armazenada em um volume e em um disk de acordo com a política de armazenamento especificada:

1. É escolhido o primeiro volume (na ordem de definição) que tenha espaço em disk suficiente para armazenar uma parte (`unreserved_space > current_part_size`) e permita armazenar partes de um determinado tamanho (`max_data_part_size_bytes > current_part_size`).
2. Dentro desse volume, é escolhido o disk seguinte àquele usado para armazenar o fragmento anterior de dados e que tenha espaço livre maior que o tamanho da parte (`unreserved_space - keep_free_space_bytes > current_part_size`).

Internamente, mutações e congelamento de partição usam [hard links](https://en.wikipedia.org/wiki/Hard_link). Hard links entre disks diferentes não são suportados; portanto, nesses casos, as partes resultantes são armazenadas nos mesmos disks que as partes iniciais.

Em segundo plano, as partes são movidas entre volumes com base na quantidade de espaço livre (parâmetro `move_factor`), de acordo com a ordem em que os volumes são declarados no arquivo de configuração.
Os dados nunca são transferidos do último para o primeiro. É possível usar as tabelas de sistema [system.part&#95;log](/pt-BR/operations/system-tables/part_log) (campo `type = MOVE_PART`) e [system.parts](/pt-BR/operations/system-tables/parts.md) (campos `path` e `disk`) para monitorar movimentações em segundo plano. Além disso, informações detalhadas podem ser encontradas nos logs do servidor.

O usuário pode forçar a movimentação de uma parte ou de uma partição de um volume para outro usando a consulta [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/pt-BR/sql-reference/statements/alter/partition); todas as restrições das operações em segundo plano são consideradas. A consulta inicia a movimentação por conta própria e não espera a conclusão das operações em segundo plano. O usuário receberá uma mensagem de erro se não houver espaço livre suficiente ou se qualquer uma das condições exigidas não for atendida.

A movimentação de dados não interfere na replicação de dados. Portanto, diferentes políticas de armazenamento podem ser especificadas para a mesma tabela em diferentes réplicas.

Após a conclusão dos merges em segundo plano e das mutações, as partes antigas são removidas somente depois de um certo tempo (`old_parts_lifetime`).
Durante esse período, elas não são movidas para outros volumes ou disks. Portanto, até que sejam finalmente removidas, elas ainda são levadas em conta na avaliação do espaço em disk ocupado.

O usuário pode atribuir novas partes grandes a diferentes disks de um volume [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) de forma equilibrada usando a configuração [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/pt-BR/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod).

<div id="table_engine-mergetree-s3">
  ## Usando armazenamento externo para armazenar dados
</div>

Os motores de tabela da família [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree.md) podem armazenar dados em `S3`, `AzureBlobStorage` e `HDFS` usando um disk dos tipos `s3`, `azure_blob_storage` e `hdfs`, respectivamente. Consulte [como configurar opções de armazenamento externo](/pt-BR/operations/storing-data.md/#configuring-external-storage) para mais detalhes.

Exemplo de [S3](https://aws.amazon.com/s3/) como armazenamento externo usando um disk do tipo `s3`.

Markup de configuração:

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

Veja também [como configurar opções de armazenamento externo](/pt-BR/operations/storing-data.md/#configuring-external-storage).

<div id="s3-multiple-volumes">
  ### Usando disks S3 com vários volumes
</div>

Disks S3 (e outros de armazenamento de objetos) podem ser usados em políticas de armazenamento com vários disks e vários volumes, da mesma forma que disks locais. Isso permite distribuir os dados entre vários buckets do S3 em um único volume (no estilo JBOD) ou configurar políticas de armazenamento em camadas com volumes S3.

Por exemplo, para distribuir os dados entre dois buckets do S3 em esquema round-robin:

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

Você também pode combinar volumes locais e S3 em uma política em camadas, por exemplo, movendo dados de um SSD local para S3 conforme eles envelhecem:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
Ao usar `use_environment_credentials` para autenticação no S3, as credenciais de ambiente (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) são compartilhadas entre todos os S3 disks. Não é possível usar credenciais de ambiente diferentes para disks diferentes. Se você precisar de credenciais diferentes para cada S3 disk, use configurações explícitas de `access_key_id` e `secret_access_key` para cada disk.
:::

É possível configurar tabelas MergeTree não replicadas em um cenário com um nó de gravação e vários de leitura em armazenamento compartilhado. Isso é viabilizado pela atualização automática da lista de partes, que pode ser configurada nos leitores. Observe que isso exige metadados de filesystem compartilhados entre réplicas (ou `table_disk = true` com um disk local da tabela). Consulte [refresh&#95;parts&#95;interval and table&#95;disk](/pt-BR/operations/storing-data.md/#refresh-parts-interval-and-table-disk).

:::note configuração de cache
As versões 22.3 a 22.7 do ClickHouse usam uma configuração de cache diferente. Consulte [using local cache](/pt-BR/operations/storing-data.md/#using-local-cache) se você estiver usando uma dessas versões.
:::

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_part` — Nome de uma parte.
* `_part_index` — Índice sequencial da parte no resultado da consulta.
* `_part_starting_offset` — Linha inicial cumulativa da parte no resultado da consulta.
* `_part_offset` — Número da linha na parte.
* `_part_granule_offset` — Número do grânulo na parte.
* `_partition_id` — Nome de uma partição.
* `_part_uuid` — Identificador único da parte (se a configuração `assign_part_uuids` do MergeTree estiver habilitada).
* `_part_data_version` — Versão de dados da parte (o número mínimo do block ou a versão da mutation).
* `_partition_value` — Valores (uma tupla) de uma expressão `partition by`.
* `_sample_factor` — Fator de amostragem (da consulta).
* `_block_number` — Número original do block da linha atribuído na inserção, preservado durante os merges quando a configuração `enable_block_number_column` está habilitada.
* `_block_offset` — Número original da linha no block atribuído na inserção, preservado durante os merges quando a configuração `enable_block_offset_column` está habilitada.
* `_disk_name` — Nome do disk usado para o armazenamento.

<div id="column-statistics">
  ## Estatísticas de colunas
</div>

<CloudNotSupportedBadge />

A declaração de estatísticas está na seção de colunas da consulta `CREATE` para tabelas da família `*MergeTree*`:

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

Também é possível manipular as estatísticas com instruções `ALTER`:

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

Essas estatísticas leves agregam informações sobre a distribuição de valores nas colunas. As estatísticas são armazenadas em cada parte e atualizadas sempre que um insert é feito.
Elas só podem ser usadas para a otimização PREWHERE se habilitarmos `set use_statistics = 1`.

<div id="part-pruning-with-statistics">
  #### Poda de partes com estatísticas
</div>

Quando `use_statistics_for_part_pruning` está habilitado, é possível usar estatísticas para a poda de partes.
Atualmente, apenas as estatísticas `MinMax` e `Basic` oferecem suporte à poda de partes. Quando essas estatísticas são definidas em uma coluna, o ClickHouse mantém os valores mínimo e máximo dessa coluna em cada parte.
A poda de partes permite evitar a leitura de partes de dados inteiras quando a condição de filtro da consulta não pode corresponder a nenhuma linha nessa parte.

**Exemplo:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### Tipos disponíveis de estatísticas de coluna
</div>

* `Basic`

  Um pacote compacto de resumos de valor único derivados de uma coluna. Dependendo do tipo da coluna, os seguintes elementos são preenchidos:

  * para qualquer coluna cujos valores sejam representados por um número (inteiros, floats, `Decimal*`, `Date*`, `DateTime*`, `Enum*`, `IPv4`, ...): o valor mínimo e o valor máximo, que permitem estimar a seletividade de filtros de intervalo e habilitam a poda de partes;
  * para colunas `String` e `FixedString`: o comprimento total, em bytes, dos valores não `NULL` (a partir do qual é possível derivar o comprimento médio da string);
  * para colunas `Nullable` e `LowCardinality(Nullable)`: a contagem de valores `NULL`, que o otimizador usa para excluir linhas `NULL` das estimativas de seletividade.

    Uma única estatística `Basic` pode preencher vários desses itens ao mesmo tempo — por exemplo, em uma coluna `Nullable(UInt32)`, ela acompanha tanto o mínimo/máximo numérico quanto a contagem de nulos. Em comparação com `MinMax`, `Basic` também funciona em colunas `String` / `FixedString` e pode ser declarada em wrappers `Nullable` de tipos como `UUID` ou `IPv6` apenas para acompanhar a contagem de nulos.

    Sintaxe: `basic`

* `MinMax`

  O valor mínimo e o valor máximo da coluna, o que permite estimar a seletividade de filtros de intervalo em colunas numéricas.

  Sintaxe: `minmax`

* `TDigest`

:::warning
Estatísticas do tipo `tdigest` têm alto custo de criação e podem potencialmente desacelerar a ingestão de dados.
:::

[TDigest](https://github.com/tdunning/t-digest) sketches que permitem calcular percentis aproximados (por exemplo, o 90º percentil) para colunas numéricas.

Sintaxe: `tdigest`

* `Uniq`

  Sketches [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) que fornecem uma estimativa de quantos valores distintos uma coluna contém.

  Sintaxe: `uniq`

* `CountMin`

:::warning
Estatísticas do tipo `countmin` têm alto custo de criação e podem potencialmente desacelerar a ingestão de dados.
:::

Sketches [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) que fornecem uma contagem aproximada da frequência de cada valor em uma coluna.

Sintaxe `countmin`

<div id="supported-data-types">
  ### Tipos de dados suportados
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | IPv4 | String ou FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

Todos os itens acima também aceitam wrappers `Nullable` e `LowCardinality(Nullable)` dos tipos listados. Além disso, `Basic` pode ser declarado em wrappers `Nullable` de tipos como `UUID` ou `IPv6` apenas para rastrear a contagem de valores nulos.

<div id="supported-operations">
  ### Operações suportadas
</div>

|          | Filtros de igualdade (==) | Filtros de intervalo (`>, >=, <, <=`) |
| -------- | ------------------------- | ------------------------------------- |
| Basic    | ✗                         | ✔ (apenas colunas numéricas)          |
| CountMin | ✔                         | ✗                                     |
| MinMax   | ✗                         | ✔ (apenas colunas numéricas)          |
| TDigest  | ✗                         | ✔ (apenas colunas numéricas)          |
| Uniq     | ✔                         | ✗                                     |

Para `Basic` em colunas `String` / `FixedString`, a estatística registra apenas o comprimento total, em bytes, dos valores não NULL
(usado para estimar o comprimento médio das strings) e a contagem de NULL;
os filtros de intervalo e a poda de partes não dependem dela.

<div id="column-level-settings">
  ## Configurações em nível de coluna
</div>

Algumas configurações do MergeTree podem ser sobrescritas no nível da coluna:

* `max_compress_block_size` — Tamanho máximo dos blocos de dados não comprimidos antes de serem comprimidos para gravação em uma tabela.
* `min_compress_block_size` — Tamanho mínimo dos blocos de dados não comprimidos necessário para compressão ao gravar a próxima mark.

Exemplo:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

As configurações em nível de coluna podem ser modificadas ou removidas usando [ALTER MODIFY COLUMN](/pt-BR/sql-reference/statements/alter/column.md), por exemplo:

* Remover `SETTINGS` da declaração da coluna:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* Altere uma configuração:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* Redefine uma ou mais configurações; isso também remove a declaração da configuração da expressão de coluna na consulta CREATE da tabela.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```