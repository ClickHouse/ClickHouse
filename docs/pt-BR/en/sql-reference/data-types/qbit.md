---
description: 'Documentação para o tipo de dado QBit no ClickHouse, que permite quantização de granularidade fina para busca vetorial aproximada'
keywords: ['qbit', 'tipo de dado']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'Tipo de dado QBit'
doc_type: 'reference'
---

O tipo de dado `QBit` reorganiza o armazenamento de vetores para tornar as buscas aproximadas mais rápidas. Em vez de armazenar juntos os elementos de cada vetor, ele agrupa as mesmas posições de dígitos binários em todos os vetores.
Isso armazena os vetores com precisão total, permitindo ao mesmo tempo que você escolha o nível de quantização de granularidade fina no momento da busca: leia menos bits para reduzir a E/S e acelerar os cálculos, ou mais bits para obter maior acurácia. Você obtém os ganhos de velocidade da redução da transferência de dados e do processamento proporcionados pela quantização, mas todos os dados originais continuam disponíveis quando necessário.

Para declarar uma coluna do tipo `QBit`, use a seguinte sintaxe:

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – o tipo de cada elemento do vetor. Os tipos permitidos são `Int8`, `BFloat16`, `Float32` e `Float64`
* `dimension` – o número de elementos em cada vetor
* `stride` – opcional. O número de dimensões armazenadas juntas em um grupo de fluxos. Quando omitido, assume por padrão `dimension` (um único grupo). Quando informado, `dimension` deve ser um múltiplo de `stride` e, quando `stride` for menor que `dimension`, `stride` deve ser um múltiplo de 8. Consulte [Strides](#strides).

<div id="creating-qbit">
  ## Criando QBit
</div>

Use o tipo `QBit` na definição de coluna da tabela:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## Convertendo arrays para QBit
</div>

Arrays são convertidos para `QBit` quando o comprimento do array corresponde à dimensão do `QBit`. O tipo de elemento do array não precisa corresponder ao tipo de elemento do `QBit`. Qualquer tipo de elemento numérico é convertido automaticamente. Isso permite mover uma coluna existente de embeddings diretamente para uma coluna `QBit`:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

A conversão também pode ser feita explicitamente com `CAST`, por exemplo, `CAST(embedding AS QBit(Float32, 8))`.

<div id="converting-qbit-to-arrays">
  ## Convertendo QBit para arrays
</div>

A conversão inversa reconstrói o vetor original a partir da representação transposta em bits; portanto, converter um `QBit` em um `Array` retorna os valores armazenados. Este é o inverso de [converter arrays para `QBit`](#converting-arrays-to-qbit):

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

O array reconstruído usa o tipo de elemento do `QBit`, e seus elementos são então convertidos para o tipo de elemento do array solicitado. Portanto, um cast que também altera o tipo de elemento, como de `QBit(Float32, N)` para `Array(Float64)`, também funciona.

Um round trip `Array` -&gt; `QBit` -&gt; `Array` não tem perda para `Int8`, `Float32` e `Float64`. Para `BFloat16`, ele corresponde a uma conversão direta para `BFloat16` — a única precisão perdida é a do próprio `BFloat16`.

Quando a `dimension` não é um múltiplo de 8, os elementos de preenchimento no final presentes na representação interna são descartados, de modo que o resultado sempre tenha exatamente `dimension` elementos.

<div id="qbit-subcolumns">
  ## Subcolunas do QBit
</div>

`QBit` implementa um padrão de acesso a subcolunas que permite acessar planos de bits individuais dos vetores armazenados. Cada posição de bit pode ser acessada usando a sintaxe `.N`, em que `N` é a posição do bit:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

O número de subcolunas acessíveis depende do tipo de elemento (e, quando há strides, do número de grupos de stride — veja [Strides](#strides)):

* `Int8`: 8 subcolunas por grupo de stride (1-8)
* `BFloat16`: 16 subcolunas por grupo de stride (1-16)
* `Float32`: 32 subcolunas por grupo de stride (1-32)
* `Float64`: 64 subcolunas por grupo de stride (1-64)

<div id="strides">
  ## Strides
</div>

Por padrão, um `QBit` armazena cada plano de bits como um único fluxo que abrange todas as `dimension` dimensões, de modo que a busca sempre leia planos de bits inteiros ao longo de todo o vetor. O parâmetro opcional `stride` particiona as `dimension` dimensões em `dimension / stride` grupos contíguos e armazena os planos de bits de cada grupo em fluxos separados. Isso permite que uma busca restrita às primeiras `D` dimensões (com `D` sendo um múltiplo de `stride`) leia apenas os fluxos dos grupos que cobrem essas dimensões — útil para [embeddings Matryoshka](https://arxiv.org/abs/2205.13147), em que as dimensões iniciais formam um embedding utilizável de menor dimensionalidade.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

Aqui, as 4096 dimensões são divididas em 4 grupos de 1024. As subcolunas seguem uma ordem com prioridade de grupo: com `BFloat16` (16 planos de bits), `vec.1` … `vec.16` são os 16 planos de bits do primeiro grupo de stride (dimensões 1–1024), `vec.17` … `vec.32` pertencem ao segundo grupo (dimensões 1025–2048), e assim por diante. Em geral, `vec.N` lê o plano de bits `(N-1) % element_size` do grupo de stride `(N-1) / element_size`.

Para executar uma busca com número reduzido de dimensões, passe o número de dimensões a serem lidas como o quarto argumento das funções de distância transpostas (veja abaixo). O vetor de referência deve ter exatamente essa quantidade de elementos, e o valor deve ser um múltiplo de `stride`.

<div id="vector-search-functions">
  ## Funções de busca vetorial
</div>

Estas são as funções de distância para busca por similaridade vetorial que usam o tipo de dados `QBit`:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

Para um `QBit` com stride, essas funções aceitam um quarto argumento opcional, `used_dims` — o número de dimensões iniciais a serem lidas — e, nesse caso, leem apenas os grupos de stride que abrangem essas dimensões:

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```