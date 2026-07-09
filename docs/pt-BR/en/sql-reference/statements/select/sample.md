---
description: 'Documentação da cláusula SAMPLE'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'Cláusula SAMPLE'
doc_type: 'reference'
---

A cláusula `SAMPLE` permite o processamento aproximado de consultas `SELECT`.

Quando a amostragem de dados está habilitada, a consulta não é executada sobre todos os dados, mas apenas sobre uma determinada fração deles (amostra). Por exemplo, se você precisa calcular estatísticas para todas as visits, basta executar a consulta sobre 1/10 de todas as visits e depois multiplicar o resultado por 10.

O processamento aproximado de consultas pode ser útil nos seguintes casos:

* Quando você tem requisitos rígidos de latência (como abaixo de 100 ms), mas não consegue justificar o custo de recursos de hardware adicionais para atendê-los.
* Quando seus dados brutos não são precisos, de modo que a aproximação não prejudique perceptivelmente a qualidade.
* Quando os requisitos de negócio visam resultados aproximados (por custo-benefício ou para oferecer resultados exatos a usuários premium).

:::note
Você só pode usar amostragem com tabelas da família [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), e somente se a expressão de amostragem tiver sido especificada durante a criação da tabela (consulte [motor MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).
:::

As características da amostragem de dados estão listadas abaixo:

* A amostragem de dados é um mecanismo determinístico. O resultado da mesma consulta `SELECT .. SAMPLE` é sempre o mesmo.
* A amostragem funciona de forma consistente em tabelas diferentes. Para tabelas com uma única chave de amostragem, uma amostra com o mesmo coeficiente sempre seleciona o mesmo subconjunto dos dados possíveis. Por exemplo, uma amostra de IDs de usuário seleciona linhas com o mesmo subconjunto de todos os IDs de usuário possíveis em tabelas diferentes. Isso significa que você pode usar a amostra em subconsultas na cláusula [IN](../../../sql-reference/operators/in.md). Além disso, você pode combinar amostras usando a cláusula [JOIN](../../../sql-reference/statements/select/join.md).
* A amostragem permite ler menos dados do disco. Observe que você deve especificar a chave de amostragem corretamente. Para mais informações, consulte [Criando uma tabela MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Para a cláusula `SAMPLE`, há suporte à seguinte sintaxe:

| Sintaxe da cláusula SAMPLE | Descrição                                                                                                                                                                                                                                                                             |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SAMPLE k`                 | Aqui, `k` é um número de 0 a 1. A consulta é executada sobre a fração `k` dos dados. Por exemplo, `SAMPLE 0.1` executa a consulta sobre 10% dos dados. [Leia mais](#sample-k)                                                                                                         |
| `SAMPLE n`                 | Aqui, `n` é um número inteiro suficientemente grande. A consulta é executada sobre uma amostra de pelo menos `n` linhas (mas não significativamente mais do que isso). Por exemplo, `SAMPLE 10000000` executa a consulta sobre um mínimo de 10.000.000 linhas. [Leia mais](#sample-n) |
| `SAMPLE k OFFSET m`        | Aqui, `k` e `m` são números de 0 a 1. A consulta é executada sobre uma amostra da fração `k` dos dados. Os dados usados na amostra são deslocados pela fração `m`. [Leia mais](#sample-k-offset-m)                                                                                    |

<div id="sample-k">
  ## SAMPLE K
</div>

Aqui, `k` é um número de 0 a 1 (há suporte tanto para notação fracionária quanto para decimal). Por exemplo, `SAMPLE 1/2` ou `SAMPLE 0.5`.

Em uma cláusula `SAMPLE k`, a amostra é extraída da fração `k` dos dados. Veja o exemplo abaixo:

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

Neste exemplo, a consulta é executada sobre uma amostra de 0,1 (10%) dos dados. Os valores das funções de agregação não são corrigidos automaticamente, portanto, para obter um resultado aproximado, o valor de `count()` é multiplicado manualmente por 10.

<div id="sample-n">
  ## SAMPLE N
</div>

Aqui, `n` é um inteiro suficientemente grande. Por exemplo, `SAMPLE 10000000`.

Nesse caso, a consulta é executada sobre uma amostra de pelo menos `n` linhas (mas não muito mais do que isso). Por exemplo, `SAMPLE 10000000` executa a consulta sobre um mínimo de 10.000.000 linhas.

Como a unidade mínima de leitura de dados é um grânulo (cujo tamanho é definido pela configuração `index_granularity`), faz sentido definir uma amostra muito maior do que o tamanho do grânulo.

Ao usar a cláusula `SAMPLE n`, você não sabe qual porcentagem relativa dos dados foi processada. Portanto, não sabe por qual coeficiente as funções de agregação devem ser multiplicadas. Use a coluna virtual `_sample_factor` para obter um resultado aproximado.

A coluna `_sample_factor` contém coeficientes relativos calculados dinamicamente. Essa coluna é criada automaticamente quando você [cria](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) uma tabela com a chave de amostragem especificada. Os exemplos de uso da coluna `_sample_factor` são mostrados abaixo.

Vamos considerar a tabela `visits`, que contém estatísticas sobre visitas ao site. O primeiro exemplo mostra como calcular o número de visualizações de página:

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

O próximo exemplo mostra como calcular o número total de visitas:

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

O exemplo abaixo mostra como calcular a duração média da sessão. Observe que não é necessário usar o coeficiente relativo para calcular os valores médios.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

<div id="sample-k-offset-m">
  ## SAMPLE K OFFSET M
</div>

Aqui, `k` e `m` são números de 0 a 1. Veja os exemplos abaixo.

**Exemplo 1**

```sql
SAMPLE 1/10
```

Neste exemplo, a amostra corresponde a 1/10 de todos os dados:

`[++------------]`

**Exemplo 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Aqui, uma amostra de 10% é retirada da segunda metade dos dados.

`[------++------]`