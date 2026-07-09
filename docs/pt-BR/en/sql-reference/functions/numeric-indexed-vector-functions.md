---
description: 'Documentação do NumericIndexedVector e suas funções'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'Funções de NumericIndexedVector'
doc_type: 'reference'
---

NumericIndexedVector é uma estrutura de dados abstrata que encapsula um vetor e implementa agregação de vetores e operações ponto a ponto. Seu método de armazenamento é o Bit-Sliced Index. Para conhecer a base teórica e os cenários de uso, consulte o artigo [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

<div id="bit-sliced-index">
  ## BSI
</div>

No método de armazenamento BSI (Bit-Sliced Index), os dados são armazenados em [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) e depois comprimidos com [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap). As operações de agregação e as operações ponto a ponto são executadas diretamente sobre os dados comprimidos, o que pode melhorar significativamente a eficiência do armazenamento e da consulta.

Um vetor contém índices e seus respectivos valores. A seguir estão algumas características e restrições dessa estrutura de dados no modo de armazenamento BSI:

* O tipo de índice pode ser `UInt8`, `UInt16` ou `UInt32`. **Nota:** Considerando o desempenho da implementação de 64 bits do Roaring Bitmap, o formato BSI não oferece suporte a `UInt64`/`Int64`.
* O tipo de valor pode ser `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32` ou `Float64`. **Nota:** O tipo de valor não é expandido automaticamente. Por exemplo, se você usar `UInt8` como tipo de valor, qualquer soma que exceda a capacidade de `UInt8` resultará em overflow, em vez de ser promovida a um tipo maior; da mesma forma, operações com inteiros produzirão resultados inteiros (por exemplo, a divisão não será convertida automaticamente em um resultado de ponto flutuante). Portanto, é importante planejar e definir o tipo de valor com antecedência. Em cenários reais, tipos de ponto flutuante (`Float32`/`Float64`) são comumente usados.
* Somente dois vetores com o mesmo tipo de índice e de valor podem realizar operações.
* O armazenamento subjacente usa Bit-Sliced Index, com bitmaps para armazenar índices. O Roaring Bitmap é a implementação específica de bitmap usada. Uma boa prática é concentrar o índice em alguns contêineres do Roaring Bitmap tanto quanto possível para maximizar a compactação e o desempenho da consulta.
* O mecanismo Bit-Sliced Index converte o valor em binário. Para tipos de ponto flutuante, a conversão usa representação de ponto fixo, o que pode levar à perda de precisão. A precisão pode ser ajustada personalizando o número de bits usados para a parte fracionária; o padrão é 24 bits, o que é suficiente para a maioria dos cenários. Você pode personalizar o número de bits inteiros e fracionários ao construir um NumericIndexedVector usando a função de agregação groupNumericIndexedVector com `-State`.
* Há três casos para índices: valor diferente de zero, valor zero e inexistente. Em NumericIndexedVector, somente valores diferentes de zero e valores zero são armazenados. Além disso, em operações ponto a ponto entre dois NumericIndexedVectors, o valor de um índice inexistente é tratado como 0. Em divisões, o resultado é zero quando o divisor é zero.

<div id="create-numeric-indexed-vector-object">
  ## Criar um objeto numericIndexedVector
</div>

Há duas maneiras de criar essa estrutura: uma é usar a função de agregação `groupNumericIndexedVector` com `-State`.
Você pode adicionar o sufixo `-if` para aceitar uma condição adicional.
A função de agregação processará apenas as linhas que atenderem à condição.
A outra é construí-lo a partir de um map usando `numericIndexedVectorBuild`.
A função `groupNumericIndexedVectorState` permite personalizar o número de bits da parte inteira e da parte fracionária por meio de parâmetros, enquanto `numericIndexedVectorBuild` não permite isso.

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

Constrói um NumericIndexedVector a partir de duas colunas de dados e retorna a soma de todos os valores no tipo `Float64`. Se o sufixo `State` for adicionado, retorna um objeto NumericIndexedVector.

**Sintaxe**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Parâmetros**

* `type`: String, opcional. Especifica o formato de armazenamento. No momento, apenas `'BSI'` tem suporte.
* `integer_bit_num`: `UInt32`, opcional. Válido para o formato de armazenamento `'BSI'`, este parâmetro indica o número de bits usados na parte inteira. Quando o tipo de índice é inteiro, o valor padrão corresponde ao número de bits usados para armazenar o índice. Por exemplo, se o tipo de índice for UInt16, o valor padrão de `integer_bit_num` será 16. Para tipos de índice Float32 e Float64, o valor padrão de integer&#95;bit&#95;num é 40; portanto, a parte inteira dos dados que pode ser representada fica no intervalo `[-2^39, 2^39 - 1]`. O intervalo permitido é `[0, 64]`.
* `fraction_bit_num`: `UInt32`, opcional. Válido para o formato de armazenamento `'BSI'`, este parâmetro indica o número de bits usados na parte fracionária. Quando o tipo de valor é inteiro, o valor padrão é 0; quando o tipo de valor é Float32 ou Float64, o valor padrão é 24. O intervalo válido é `[0, 24]`.
* Há também a restrição de que o intervalo válido de integer&#95;bit&#95;num + fraction&#95;bit&#95;num é [0, 64].
* `col1`: A coluna de índice. Tipos compatíveis: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
* `col2`: A coluna de valor. Tipos compatíveis: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**Valor de retorno**

Um valor `Float64` que representa a soma de todos os valores.

**Exemplo**

Dados de teste:

```text
UserID  PlayTime
1       10
2       20
3       30
```

Consulta &amp; Resultado:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
A documentação abaixo é gerada com base na tabela de sistema `system.functions`.
:::

{/* 
  as tags abaixo são utilizadas para gerar a documentação a partir das tabelas de sistema e não devem ser removidas.
  Para mais detalhes, consulte https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }