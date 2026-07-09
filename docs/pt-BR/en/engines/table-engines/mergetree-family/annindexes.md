---
description: 'Documentação sobre busca vetorial exata e aproximada'
keywords: ['busca por similaridade vetorial', 'ann', 'knn', 'hnsw', 'índices', 'índice', 'vizinho mais próximo', 'busca vetorial']
sidebar_label: 'Busca vetorial exata e aproximada'
slug: /engines/table-engines/mergetree-family/annindexes
title: 'Busca vetorial exata e aproximada'
doc_type: 'guide'
---

O problema de encontrar os N pontos mais próximos de um determinado ponto em um espaço multidimensional (vetorial) é conhecido como [busca do vizinho mais próximo](https://en.wikipedia.org/wiki/Nearest_neighbor_search) ou, resumidamente, busca vetorial.
Existem duas abordagens gerais para realizar a busca vetorial:

* A busca vetorial exata calcula a distância entre o ponto fornecido e todos os pontos no espaço vetorial. Isso garante a melhor precisão possível, ou seja, os pontos retornados são garantidamente os vizinhos mais próximos reais. Como o espaço vetorial é explorado exaustivamente, a busca vetorial exata pode ser lenta demais para uso no mundo real.
* A busca vetorial aproximada se refere a um conjunto de técnicas (por exemplo, estruturas de dados especiais, como grafos e florestas aleatórias) que calculam resultados muito mais rapidamente do que a busca vetorial exata. A precisão dos resultados normalmente é &quot;boa o suficiente&quot; para uso prático. Muitas técnicas aproximadas fornecem parâmetros para ajustar o equilíbrio entre a precisão dos resultados e o tempo de busca.

Uma busca vetorial (exata ou aproximada) pode ser escrita em SQL da seguinte forma:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

Os pontos no espaço vetorial são armazenados em uma coluna `vectors` do tipo Array, por exemplo [Array(Float64)](../../../sql-reference/data-types/array.md), [Array(Float32)](../../../sql-reference/data-types/array.md) ou [Array(BFloat16)](../../../sql-reference/data-types/array.md).
O vetor de referência é um Array constante e é definido como uma expressão de tabela comum.
`<DistanceFunction>` calcula a distância entre o ponto de referência e todos os pontos armazenados.
Para isso, pode ser usada qualquer [função de distância](/pt-BR/sql-reference/functions/distance-functions) disponível.
`<N>` especifica quantos vizinhos devem ser retornados.

<div id="exact-nearest-neighbor-search">
  ## Busca vetorial exata
</div>

Uma busca vetorial exata pode ser realizada usando a consulta SELECT acima, sem alterações.
O tempo de execução dessas consultas geralmente é proporcional ao número de vetores armazenados e à sua dimensão, ou seja, ao número de elementos no array.
Além disso, como o ClickHouse executa uma varredura por força bruta em todos os vetores, o tempo de execução também depende do número de threads usadas pela consulta (consulte a configuração [max&#95;threads](../../../operations/settings/settings.md#max_threads)).

<div id="exact-nearest-neighbor-search-example">
  ### Exemplo
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

retorna

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## Busca vetorial aproximada
</div>

<div id="vector-similarity-index">
  ### Índices de similaridade vetorial
</div>

O ClickHouse fornece um índice especial de &quot;similaridade vetorial&quot; para realizar busca vetorial aproximada.

:::note
Os índices de similaridade vetorial estão disponíveis no ClickHouse a partir da versão 25.8.
Se você tiver problemas, abra uma issue no [repositório do ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

<div id="creating-a-vector-similarity-index">
  #### Criando um índice de similaridade vetorial
</div>

Um índice de similaridade vetorial pode ser criado em uma nova tabela assim:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

Como alternativa, para adicionar um índice de similaridade vetorial a uma tabela existente:

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

Índices de similaridade vetorial são tipos especiais de índices de salto (veja [aqui](mergetree.md#table_engine-mergetree-data_skipping-indexes) e [aqui](../../../optimize/skipping-indexes)).
Portanto, o comando `ALTER TABLE` acima faz com que o índice seja construído apenas para os novos dados inseridos na tabela futuramente.
Para construir o índice também para os dados existentes, é necessário materializá-lo:

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

A função `<distance_function>` deve ser

* `L2Distance`, a [distância euclidiana](https://en.wikipedia.org/wiki/Euclidean_distance), que representa o comprimento do segmento de reta entre dois pontos no espaço euclidiano,
* `cosineDistance`, a [distância de cosseno](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance), que representa o ângulo entre dois vetores não nulos, ou
* `dotProduct`, o [produto escalar](https://en.wikipedia.org/wiki/Dot_product) (produto interno), que representa a soma dos produtos elemento a elemento de dois vetores. Equivalente a `cosineDistance` em dados normalizados.

Para dados normalizados, `L2Distance` geralmente é a melhor escolha; caso contrário, `cosineDistance` é recomendada para compensar a escala.

:::note
Para as funções de distância `L2Distance` e `cosineDistance`, um valor menor indica maior similaridade, enquanto para `dotProduct`, um valor maior indica maior similaridade.
Como resultado, índices vetoriais com `L2Distance` e `cosineDistance` só podem ser usados por consultas `SELECT [...] ORDER BY [...] ASC` (`ASC` é o padrão para `ORDER BY`), enquanto índices vetoriais construídos para `dotProduct` só podem ser usados por consultas `SELECT [...] ORDER BY [...] DESC`.
:::

`<dimensions>` especifica a cardinalidade do array (número de elementos) na coluna subjacente.
Se o ClickHouse encontrar um array com cardinalidade diferente durante a criação do índice, o índice é descartado e um erro é retornado.

O parâmetro opcional GRANULARITY `<N>` refere-se ao tamanho dos grânulos do índice (consulte [aqui](../../../optimize/skipping-indexes)).
Ao contrário dos skip indexes regulares, que utilizam uma granularidade de índice padrão de 1, os vector similarity indexes utilizam 100 milhões como granularidade de índice padrão.
Esse valor garante que apenas alguns índices sejam criados internamente, mesmo para partes grandes.
Recomendamos alterar a granularidade do índice apenas para usuários avançados que compreendam as implicações do que estão fazendo (consulte [abaixo](#differences-to-regular-skipping-indexes)).

Os índices de similaridade vetorial são genéricos no sentido de que podem acomodar diferentes métodos de busca aproximada.
O método efetivamente utilizado é especificado pelo parâmetro `<type>`.
Atualmente, o único método disponível é o HNSW ([artigo acadêmico](https://arxiv.org/abs/1603.09320)), uma técnica popular e de ponta para busca vetorial aproximada baseada em grafos hierárquicos de proximidade.
Se o HNSW for utilizado como tipo, os usuários podem, opcionalmente, especificar parâmetros adicionais específicos do HNSW:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

Os seguintes parâmetros específicos do HNSW estão disponíveis:

* `<quantization>` controla a quantização dos vetores no grafo de proximidade. Os valores possíveis são `f64`, `f32`, `f16`, `bf16`, `i8` ou `b1`. O valor padrão é `bf16`. Observe que esse parâmetro não afeta a representação dos vetores na coluna subjacente.
* `<hnsw_max_connections_per_layer>` controla o número de vizinhos por node do grafo, também conhecido como hiperparâmetro `M` do HNSW. O valor padrão é `32`. O valor `0` significa usar o valor padrão.
* `<hnsw_candidate_list_size_for_construction>` controla o tamanho da lista dinâmica de candidatos durante a construção do grafo HNSW, também conhecido como hiperparâmetro `ef_construction` do HNSW. O valor padrão é `128`. O valor `0` significa usar o valor padrão.

Os valores padrão de todos os parâmetros específicos do HNSW funcionam razoavelmente bem na maioria dos casos de uso.
Portanto, não recomendamos personalizar os parâmetros específicos do HNSW.

Aplicam-se também as seguintes restrições:

* Índices de similaridade vetorial só podem ser criados em colunas do tipo [Array(Float32)](../../../sql-reference/data-types/array.md), [Array(Float64)](../../../sql-reference/data-types/array.md) ou [Array(BFloat16)](../../../sql-reference/data-types/array.md). Arrays de floats anuláveis e de baixa cardinalidade, como `Array(Nullable(Float32))` e `Array(LowCardinality(Float32))`, não são permitidos.
* Índices de similaridade vetorial devem ser criados sobre uma única coluna.
* Índices de similaridade vetorial podem ser criados sobre expressões calculadas (por exemplo, `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`), mas esses índices não podem ser usados posteriormente para busca aproximada de vizinhos.
* Índices de similaridade vetorial exigem que todos os arrays na coluna subjacente tenham `<dimension>` elementos — isso é verificado durante a criação do índice. Para detectar violações desse requisito o quanto antes, os usuários podem adicionar uma [restrição](/pt-BR/sql-reference/statements/create/table.md#constraints) para a coluna vetorial, por exemplo, `CONSTRAINT same_length CHECK length(vectors) = 256`.
* Da mesma forma, os valores de array na coluna subjacente não podem estar vazios (`[]`) nem ter valor padrão (também `[]`).

**Estimando o consumo de armazenamento e memória**

Um vetor gerado para uso com um modelo de IA típico (por exemplo, um Large Language Model, [LLMs](https://en.wikipedia.org/wiki/Large_language_model)) consiste em centenas ou milhares de valores de ponto flutuante.
Assim, um único valor vetorial pode consumir vários quilobytes de memória.
Os usuários que quiserem estimar o armazenamento necessário para a coluna vetorial subjacente na tabela, bem como a memória principal necessária para o índice de similaridade vetorial, podem usar as duas fórmulas abaixo:

Consumo de armazenamento da coluna vetorial na tabela (não comprimido):

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

Exemplo com o [conjunto de dados dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

O índice de similaridade vetorial deve ser carregado completamente do disco para a memória principal para realizar buscas.
Da mesma forma, o índice vetorial também é construído inteiramente em memória e, em seguida, salvo em disco.

Consumo de memória necessário para carregar um índice vetorial:

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

Exemplo com o [conjunto de dados dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M):

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

A fórmula acima não considera a memória adicional necessária para que os índices de similaridade vetorial aloquem estruturas de dados em tempo de execução, como buffers pré-alocados e caches.

<div id="using-a-vector-similarity-index">
  #### Usando um índice de similaridade vetorial
</div>

:::note
Para usar índices de similaridade vetorial, a configuração [compatibility](../../../operations/settings/settings.md) deve estar definida como `''` (o valor padrão), `'25.1'` ou uma versão mais recente.
:::

Índices de similaridade vetorial oferecem suporte a consultas SELECT neste formato:

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

O otimizador de consulta do ClickHouse tenta identificar o modelo de consulta acima e aproveitar os índices de similaridade vetorial disponíveis.
Uma consulta só pode usar um índice de similaridade vetorial se a função de distância na consulta SELECT for a mesma da definição do índice.

Usuários avançados podem fornecer um valor personalizado para a configuração [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) (também conhecida como o hiperparâmetro HNSW &quot;ef&#95;search&quot;) para ajustar o tamanho da lista de candidatos durante a busca (por exemplo, `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
O valor padrão da configuração, 256, funciona bem na maioria dos casos de uso.
Valores mais altos dessa configuração significam maior precisão, ao custo de um desempenho mais lento.

Se a consulta puder usar um índice de similaridade vetorial, o ClickHouse verifica se o LIMIT `<N>` fornecido nas consultas SELECT está dentro de limites razoáveis.
Mais especificamente, um erro é retornado se `<N>` for maior que o valor da configuração [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries), cujo valor padrão é 100.
Valores de LIMIT muito altos podem tornar as buscas mais lentas e geralmente indicam uso incorreto.

Para verificar se uma consulta SELECT usa um índice de similaridade vetorial, você pode prefixá-la com `EXPLAIN indexes = 1`.

Como exemplo, a consulta

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

pode retornar

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Neste exemplo, 1 milhão de vetores no [conjunto de dados dbpedia](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M), cada um com dimensão 1536, são armazenados em 575 grânulos, ou seja, 1,7 mil linhas por grânulo.
A consulta solicita 10 vizinhos, e o índice de similaridade vetorial encontra esses 10 vizinhos em 10 grânulos distintos.
Esses 10 grânulos serão lidos durante a execução da consulta.

Índices de similaridade vetorial são utilizados se a saída contiver `Skip`, além do nome e do tipo do índice vetorial (no exemplo, `idx` e `vector_similarity`).
Nesse caso, o índice de similaridade vetorial descartou dois de quatro grânulos, ou seja, 50% dos dados.
Quanto mais grânulos puderem ser descartados, mais eficaz será o uso do índice.

:::tip
Para forçar o uso do índice, você pode executar a consulta SELECT com a configuração [force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) (forneça o nome do índice como valor da configuração).
:::

**Pós-filtragem e pré-filtragem**

Opcionalmente, os usuários podem especificar uma cláusula `WHERE` com condições de filtro adicionais para a consulta SELECT.
O ClickHouse avaliará essas condições de filtro usando a estratégia de pós-filtragem ou pré-filtragem.
Em resumo, ambas as estratégias determinam a ordem em que os filtros são avaliados:

* Pós-filtragem significa que o índice de similaridade vetorial é avaliado primeiro; depois, o ClickHouse avalia os filtros adicionais especificados na cláusula `WHERE`.
* Pré-filtragem significa que a ordem de avaliação dos filtros é a inversa.

As estratégias têm diferentes trade-offs:

* A pós-filtragem tem o problema geral de poder retornar menos linhas do que o número solicitado na cláusula `LIMIT <N>`. Essa situação ocorre quando uma ou mais linhas de resultado retornadas pelo índice de similaridade vetorial não atendem aos filtros adicionais.
* A pré-filtragem, em geral, ainda é um problema sem solução. Alguns bancos de dados vetoriais especializados oferecem algoritmos de pré-filtragem, mas a maioria dos bancos de dados relacionais (incluindo o ClickHouse) recorrerá à busca exata por vizinhos, isto é, a uma varredura por força bruta, sem índice.

A estratégia usada depende da condição de filtro.

*Filtros adicionais fazem parte da chave de partição*

Se a condição de filtro adicional fizer parte da chave de partição, o ClickHouse aplicará o pruning de partições.
Como exemplo, uma tabela é particionada por intervalo com base na coluna `year`, e a seguinte consulta é executada:

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

O ClickHouse descartará todas as partições, exceto a de 2025.

*Filtros adicionais não podem ser avaliados usando índices*

Se condições de filtro adicionais não puderem ser avaliadas usando índices (índice de chave primária, skipping index), o ClickHouse aplicará pós-filtragem.

*Filtros adicionais podem ser avaliados usando o índice de chave primária*

Se condições de filtro adicionais puderem ser avaliadas usando a [chave primária](mergetree.md#primary-key) (ou seja, formarem um prefixo da chave primária) e

* a condição de filtro eliminar pelo menos uma linha dentro de uma part, o ClickHouse recorrerá à pré-filtragem para os intervalos &quot;sobreviventes&quot; dentro da part,
* a condição de filtro não eliminar nenhuma linha dentro de uma part, o ClickHouse fará pós-filtragem para a part.

Na prática, este último caso é pouco provável.

*Filtros adicionais podem ser avaliados usando skipping index*

Se condições de filtro adicionais puderem ser avaliadas usando [skipping indexes](mergetree.md#table_engine-mergetree-data_skipping-indexes) (índice minmax, índice set etc.), o ClickHouse fará pós-filtragem.
Nesses casos, o índice de similaridade vetorial é avaliado primeiro, pois é esperado que ele elimine mais linhas do que os outros skipping indexes.

Para ter um controle mais preciso sobre pós-filtragem vs. pré-filtragem, é possível usar duas configurações:

A configuração [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy) (padrão: `auto`, que implementa as heurísticas acima) pode ser definida como `prefilter`.
Isso é útil para forçar a pré-filtragem nos casos em que as condições de filtro adicionais são extremamente seletivas.
Por exemplo, a consulta a seguir pode se beneficiar da pré-filtragem:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

Supondo que apenas um número muito pequeno de livros custe menos de 2 dólares, a pós-filtragem pode retornar zero linhas, porque as 10 correspondências mais relevantes retornadas pelo índice vetorial podem ter preço acima de 2 dólares.
Ao forçar a pré-filtragem (adicione `SETTINGS vector_search_filter_strategy = 'prefilter'` à consulta), o ClickHouse primeiro encontra todos os livros com preço inferior a 2 dólares e depois executa uma busca vetorial por força bruta entre os livros encontrados.

Como alternativa para resolver o problema acima, a configuração [vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (padrão: `1.0`, máximo: `1000.0`) pode ser definida com um valor &gt; `1.0` (por exemplo, `2.0`).
O número de vizinhos mais próximos buscados no índice vetorial é multiplicado pelo valor da configuração, e depois o filtro adicional é aplicado a essas linhas para retornar o número de linhas especificado em LIMIT.
Como exemplo, podemos executar a consulta novamente, mas com o multiplicador `3.0`:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

O ClickHouse buscará 3,0 x 10 = 30 vizinhos mais próximos no índice vetorial em cada parte e, depois, aplicará os filtros adicionais.
Apenas os dez vizinhos mais próximos serão retornados.
Vale notar que definir `vector_search_index_fetch_multiplier` pode mitigar o problema, mas, em casos extremos (condição WHERE muito seletiva), ainda é possível que sejam retornadas menos de N linhas solicitadas.

**Repontuação**

Os skip indexes no ClickHouse geralmente filtram no nível de grânulo, ou seja, uma busca em um skip index (internamente) retorna uma lista de grânulos com possível correspondência, o que reduz a quantidade de dados lidos na varredura subsequente.
Isso funciona bem para skip indexes em geral, mas, no caso dos índices de similaridade vetorial, cria uma &quot;incompatibilidade de granularidade&quot;.
Em mais detalhes, o índice de similaridade vetorial determina os números das linhas dos N vetores mais similares para um determinado vetor de referência, mas depois precisa extrapolar esses números de linha para números de grânulos.
O ClickHouse então carrega esses grânulos do disco e repete o cálculo de distância para todos os vetores nesses grânulos.
Essa etapa é chamada de rescoring e, embora possa teoricamente melhorar a precisão — lembre-se de que o índice de similaridade vetorial retorna apenas um resultado *aproximado* —, ela claramente não é ideal em termos de desempenho.

Por isso, o ClickHouse fornece uma otimização que desabilita o rescoring e retorna os vetores mais similares e suas distâncias diretamente do índice.
A otimização vem habilitada por padrão; consulte a configuração [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring).
Em linhas gerais, ela funciona da seguinte forma: o ClickHouse disponibiliza os vetores mais similares e suas distâncias como uma coluna virtual `_distances`.
Para ver isso, execute uma consulta de busca vetorial com `EXPLAIN header = 1`:

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
Uma consulta executada sem rescoring (`vector_search_with_rescoring = 0`) e com réplicas paralelas habilitadas pode voltar a usar rescoring.
:::

<div id="performance-tuning">
  #### Ajuste de desempenho
</div>

**Ajuste da compressão**

Em praticamente todos os casos de uso, os vetores na coluna subjacente são densos e não se comprimem bem.
Como resultado, a [compressão](/pt-BR/sql-reference/statements/create/table.md#column_compression_codec) torna mais lentas as inserções e leituras na coluna de vetores.
Por isso, recomendamos desativar a compressão.
Para fazer isso, especifique `CODEC(NONE)` para a coluna de vetores assim:

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**Ajustando a criação de índices**

O ciclo de vida dos índices de similaridade vetorial está vinculado ao ciclo de vida das partes.
Em outras palavras, sempre que uma nova parte com um índice de similaridade vetorial definido é criada, o índice também é criado.
Isso normalmente acontece quando os dados são [inseridos](https://clickhouse.com/docs/guides/inserting-data) ou durante [mesclagens](https://clickhouse.com/docs/merges).
Infelizmente, o HNSW é conhecido pelo longo tempo de criação de índices, o que pode tornar inserts e merges significativamente mais lentos.
Idealmente, os índices de similaridade vetorial só devem ser usados se os dados forem imutáveis ou raramente alterados.

Para acelerar a criação de índices, as seguintes técnicas podem ser usadas:

Primeiro, a criação de índices pode ser paralelizada.
O número máximo de threads para criação de índices pode ser configurado usando a configuração de servidor [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/pt-BR/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size).
Para obter o melhor desempenho, o valor dessa configuração deve ser ajustado para o número de núcleos de CPU.

Segundo, para acelerar instruções INSERT, os usuários podem desativar a criação de skipping index em partes recém-inseridas usando a configuração de sessão [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert).
As consultas SELECT nessas partes recorrerão à busca exata.
Como as partes inseridas tendem a ser pequenas em comparação com o tamanho total da tabela, espera-se que o impacto disso no desempenho seja desprezível.

Terceiro, para acelerar merges, os usuários podem desativar a criação de skipping index em partes mescladas usando a configuração de sessão [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge).
Isso, em conjunto com a instrução [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index), fornece controle explícito sobre o ciclo de vida dos índices de similaridade vetorial.
Por exemplo, a criação de índices pode ser adiada até que todos os dados tenham sido ingeridos ou até um período de baixa carga do sistema, como o fim de semana.

**Ajustando o uso de índices**

As consultas SELECT precisam carregar os índices de similaridade vetorial na memória principal para usá-los.
Para evitar que o mesmo índice de similaridade vetorial seja carregado repetidamente na memória principal, o ClickHouse fornece um cache em memória dedicado para esses índices.
Quanto maior esse cache, menos carregamentos desnecessários ocorrerão.
O tamanho máximo do cache pode ser configurado usando a configuração de servidor [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size).
Por padrão, o cache pode chegar a 5 GB.

As seguintes mensagens de log (`system.text_log`) indicam que o índice de similaridade vetorial está sendo carregado.
Se essas mensagens aparecerem repetidamente em diferentes consultas de busca vetorial, isso indica que o tamanho do cache está baixo demais.

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
O cache do índice de similaridade vetorial armazena grânulos de índice vetorial.
Se os grânulos individuais de índice vetorial forem maiores que o tamanho do cache, eles não serão armazenados em cache.
Portanto, calcule o tamanho do índice vetorial (com base na fórmula em &quot;Estimativa de armazenamento e consumo de memória&quot; ou em [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)) e dimensione o cache de acordo.
:::

*Reiteramos que verificar e, se necessário, aumentar o cache do índice vetorial deve ser a primeira etapa ao investigar consultas lentas de busca vetorial.*

O tamanho atual do cache do índice de similaridade vetorial é mostrado em [system.metrics](../../../operations/system-tables/metrics.md):

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

Os acertos e as falhas no cache de uma consulta com um determinado ID de consulta podem ser obtidos em [system.query&#95;log](../../../operations/system-tables/query_log.md):

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

Para casos de uso em produção, recomendamos dimensionar o cache de modo que todos os índices vetoriais permaneçam na memória o tempo todo.

**Ajuste da quantização**

[Quantização](https://huggingface.co/blog/embedding-quantization) é uma técnica para reduzir o uso de memória dos vetores e os custos computacionais de construir e percorrer índices vetoriais.
Os índices vetoriais do ClickHouse oferecem suporte às seguintes opções de quantização:

| Quantização   | Nome                        | Armazenamento por dimensão |
| ------------- | --------------------------- | -------------------------- |
| f32           | Precisão simples            | 4 bytes                    |
| f16           | Meia precisão               | 2 bytes                    |
| bf16 (padrão) | Meia precisão (brain float) | 2 bytes                    |
| i8            | Precisão de 1/4             | 1 byte                     |
| b1            | Binária                     | 1 bit                      |

A quantização reduz a precisão das buscas vetoriais em comparação com a busca nos valores originais de ponto flutuante com precisão total (`f32`).
No entanto, na maioria dos conjuntos de dados, a quantização brain float de meia precisão (`bf16`) resulta em perda de precisão insignificante; por isso, os índices de similaridade vetorial usam essa técnica por padrão.
A quantização de precisão de 1/4 (`i8`) e a quantização binária (`b1`) causam perda de precisão considerável nas buscas vetoriais.
Recomendamos ambas apenas se o tamanho do índice de similaridade vetorial for significativamente maior que a DRAM disponível.
Nesse caso, também sugerimos habilitar o rescoring ([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier), [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) para melhorar a acurácia.
A quantização binária é recomendada apenas para 1) embeddings normalizados (isto é, comprimento do vetor = 1; os modelos da OpenAI geralmente são normalizados) e 2) se a distância cosseno for usada como função de distância.
A quantização binária usa internamente a distância de Hamming para construir e percorrer o grafo de proximidade.
A etapa de rescoring usa os vetores originais com precisão total armazenados na tabela para identificar os vizinhos mais próximos por meio da distância cosseno.

**Ajuste da transferência de dados**

O vetor de referência em uma consulta de busca vetorial é fornecido pelo usuário e, em geral, obtido por meio de uma chamada a um Large Language Model (LLM).
Um código Python típico que executa uma busca vetorial no ClickHouse pode ser assim

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

Vetores de embedding (`search_v` no trecho acima) podem ter um número muito grande de dimensões.
Por exemplo, a OpenAI oferece modelos que geram vetores de embeddings com 1536 ou até 3072 dimensões.
No código acima, o driver Python do ClickHouse substitui o vetor de embedding por uma string legível por humanos e, em seguida, envia a consulta SELECT inteira como uma string.
Supondo que o vetor de embedding seja composto por 1536 valores de ponto flutuante de precisão simples, a string enviada chega a 20 kB de comprimento.
Isso gera alto uso de CPU para tokenização, parsing e milhares de conversões de string para float.
Além disso, isso também exige um espaço considerável no arquivo de log do servidor ClickHouse, causando inchaço em `system.query_log`.

Observe que a maioria dos modelos de LLM retorna um vetor de embedding como uma lista ou um array NumPy de floats nativos.
Portanto, recomendamos que aplicações Python façam bind do parâmetro do vetor de referência em forma binária usando o estilo a seguir:

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

No exemplo, o vetor de referência é enviado tal como está, em formato binário, e reinterpretado como um array de floats no servidor.
Isso economiza tempo de CPU no servidor e evita o aumento dos logs do servidor e de `system.query_log`.

<div id="administration">
  #### Administração e monitoramento
</div>

O tamanho no disco dos índices de similaridade vetorial pode ser obtido em [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices):

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

Exemplo de saída:

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### Diferenças em relação aos índices de skipping regulares
</div>

Assim como os [índices de skipping](/pt-BR/optimize/skipping-indexes) regulares, os índices de similaridade vetorial são construídos sobre grânulos, e cada bloco indexado consiste em `GRANULARITY = [N]` grânulos (`[N]` = 1 por padrão para índices de skipping normais).
Por exemplo, se a granularidade do índice primário da tabela for 8192 (configuração `index_granularity = 8192`) e `GRANULARITY = 2`, então cada bloco indexado conterá 16384 linhas.
No entanto, estruturas de dados e algoritmos para busca aproximada de vizinhos são inerentemente orientados por linhas.
Eles armazenam uma representação compacta de um conjunto de linhas e também retornam linhas para consultas de busca vetorial.
Isso gera algumas diferenças um tanto contraintuitivas na forma como os índices de similaridade vetorial se comportam em comparação com os índices de skipping normais.

Quando um usuário define um índice de similaridade vetorial em uma coluna, o ClickHouse cria internamente um &quot;subíndice&quot; de similaridade vetorial para cada bloco de índice.
O subíndice é &quot;local&quot; no sentido de que conhece apenas as linhas do bloco de índice ao qual pertence.
No exemplo anterior, supondo que uma coluna tenha 65536 linhas, obtemos quatro blocos de índice (abrangendo oito grânulos) e um subíndice de similaridade vetorial para cada bloco de índice.
Em teoria, um subíndice é capaz de retornar diretamente as linhas com os N pontos mais próximos dentro do seu bloco de índice.
No entanto, como o ClickHouse carrega dados do disco para a memória na granularidade dos grânulos, os subíndices extrapolam as linhas correspondentes para esse nível de granularidade.
Isso é diferente dos índices de skipping regulares, que pulam dados na granularidade dos blocos de índice.

O parâmetro `GRANULARITY` determina quantos subíndices de similaridade vetorial são criados.
Valores maiores de `GRANULARITY` significam menos subíndices de similaridade vetorial, porém maiores, até o ponto em que uma coluna (ou a data part de uma coluna) tenha apenas um único subíndice.
Nesse caso, o subíndice tem uma visão &quot;global&quot; de todas as linhas da coluna e pode retornar diretamente todos os grânulos da coluna (parte) com linhas relevantes (há, no máximo, `LIMIT [N]` desses grânulos).
Em uma segunda etapa, o ClickHouse carregará esses grânulos e identificará as melhores linhas de fato, realizando um cálculo de distância por força bruta sobre todas as linhas dos grânulos.
Com um valor pequeno de `GRANULARITY`, cada subíndice retorna até `LIMIT N` grânulos.
Como resultado, mais grânulos precisam ser carregados e pós-filtrados.
Observe que, em ambos os casos, a precisão da busca é igualmente boa; apenas o desempenho do processamento difere.
Em geral, recomenda-se usar um `GRANULARITY` alto para índices de similaridade vetorial e recorrer a valores menores de `GRANULARITY` apenas em caso de problemas, como consumo excessivo de memória pelas estruturas de similaridade vetorial.
Se nenhum `GRANULARITY` tiver sido especificado para índices de similaridade vetorial, o valor padrão será 100 milhões.

<div id="approximate-nearest-neighbor-search-example">
  #### Exemplo
</div>

Consultas:

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

Outros conjuntos de dados de exemplo que usam busca vetorial aproximada:

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

Uma abordagem comum para acelerar a busca vetorial exata é usar um [tipo de dado float](../../../sql-reference/data-types/float.md) com menor precisão.
Por exemplo, se os vetores forem armazenados como `Array(BFloat16)` em vez de `Array(Float32)`, o tamanho dos dados será reduzido pela metade, e o tempo de execução da consulta tende a diminuir na mesma proporção.
Esse método é conhecido como quantização. Embora acelere o processamento, ele pode reduzir a precisão dos resultados, mesmo realizando uma varredura exaustiva de todos os vetores.

Com a quantização tradicional, perdemos precisão tanto na busca quanto no armazenamento dos dados. No exemplo acima, armazenaríamos `BFloat16` em vez de `Float32`, o que significa que nunca poderíamos fazer uma busca mais precisa depois, mesmo se quiséssemos. Uma alternativa é armazenar duas cópias dos dados: uma quantizada e outra com precisão total. Embora isso funcione, exige armazenamento redundante. Considere um cenário em que temos `Float64` como dado original e queremos executar buscas com diferentes níveis de precisão (16 bits, 32 bits ou 64 bits completos). Precisaríamos armazenar três cópias separadas dos dados.

O ClickHouse oferece o tipo de dado Quantized Bit (`QBit`), que resolve essas limitações ao:

1. Armazenar os dados originais com precisão total.
2. Permitir que a precisão da quantização seja especificada em tempo de consulta.

Isso é feito armazenando os dados em um formato agrupado por bits (ou seja, todos os i-ésimos bits de todos os vetores são armazenados juntos), permitindo leituras apenas no nível de precisão solicitado. Assim, você obtém os ganhos de velocidade proporcionados pela redução de E/S e de processamento da quantização, sem abrir mão da disponibilidade dos dados originais quando necessário. Quando a precisão máxima é selecionada, a busca se torna exata.

Para declarar uma coluna do tipo `QBit`, use a seguinte sintaxe:

```sql
column_name QBit(element_type, dimension[, stride])
```

Em que:

* `element_type` – o tipo de cada elemento do vetor. Os tipos compatíveis são `Int8`, `BFloat16`, `Float32` e `Float64`
* `dimension` – o número de elementos em cada vetor
* `stride` – opcional. Um divisor de `dimension` que particiona as dimensões em `dimension / stride` grupos contíguos armazenados em streams separados, de modo que uma busca apenas nas dimensões iniciais leia menos streams (útil para embeddings Matryoshka). O padrão é `dimension`, caso em que o tipo é byte a byte idêntico a um `QBit` sem stride. Consulte a [página do tipo de dados `QBit`](/pt-BR/sql-reference/data-types/qbit) para mais detalhes.

<div id="qbit-create">
  #### Criando uma tabela `QBit` e adicionando dados
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### Busca vetorial com `QBit`
</div>

Vamos encontrar os vizinhos mais próximos de um vetor que representa a palavra &#39;lemon&#39; usando a distância L2. O terceiro parâmetro da função de distância especifica a precisão em bits — valores mais altos oferecem maior precisão, mas exigem mais processamento.

Você pode encontrar todas as funções de distância disponíveis para `QBit` [aqui](../../../sql-reference/data-types/qbit.md#vector-search-functions).

**Busca com precisão total (64 bits):**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**Busca com precisão reduzida:**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

Observe que, com a quantização de 12 bits, obtemos uma boa aproximação das distâncias com uma execução de consulta mais rápida. A ordenação relativa permanece bastante consistente, com &#39;apple&#39; ainda sendo a correspondência mais próxima.

<div id="qbit-performance">
  #### Considerações de desempenho
</div>

O ganho de desempenho do `QBit` vem da redução das operações de E/S, já que menos dados precisam ser lidos do armazenamento ao usar uma precisão menor. Além disso, quando o `QBit` contém dados `Float32`, se o parâmetro de precisão for 16 ou menos, há benefícios adicionais devido à redução no processamento. O parâmetro de precisão controla diretamente o equilíbrio entre exatidão e velocidade:

* **Maior precisão** (mais próxima da largura dos dados originais): resultados mais precisos, consultas mais lentas
* **Menor precisão**: consultas mais rápidas com resultados aproximados, menor uso de memória

<div id="references">
  ### Referências
</div>

Posts do blog:

* [Busca vetorial com ClickHouse - Parte 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [Busca vetorial com ClickHouse - Parte 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [Criamos um mecanismo de busca vetorial que permite escolher a precisão no momento da consulta](https://clickhouse.com/blog/qbit-vector-search)