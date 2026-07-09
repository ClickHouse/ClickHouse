---
description: 'Documentação da cláusula PREWHERE'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'Cláusula PREWHERE'
doc_type: 'reference'
---

PREWHERE é uma otimização para aplicar a filtragem com mais eficiência. Ela vem habilitada por padrão, mesmo que a cláusula `PREWHERE` não seja especificada explicitamente. Ela funciona movendo automaticamente parte da condição [WHERE](../../../sql-reference/statements/select/where.md) para a etapa de prewhere. O papel da cláusula `PREWHERE` é apenas controlar essa otimização se você achar que sabe fazer isso melhor do que o comportamento padrão.

Com a otimização de prewhere, primeiro são lidas apenas as colunas necessárias para executar a expressão de prewhere. Em seguida, são lidas as outras colunas necessárias para executar o restante da consulta, mas somente nos blocos em que a expressão de prewhere é `true` para pelo menos algumas linhas. Se houver muitos blocos em que a expressão de prewhere é `false` para todas as linhas, e o prewhere precisar de menos colunas do que outras partes da consulta, isso geralmente permite ler muito menos dados do disco para executar a consulta.

<div id="controlling-prewhere-manually">
  ## Controle manual do PREWHERE
</div>

A cláusula tem o mesmo significado da cláusula `WHERE`. A diferença está em quais dados são lidos da tabela. Ao controlar manualmente o `PREWHERE` para condições de filtragem usadas por uma minoria das colunas na consulta, mas que oferecem uma filtragem forte dos dados, você reduz o volume de dados a ser lido.

Uma consulta pode especificar `PREWHERE` e `WHERE` simultaneamente. Nesse caso, `PREWHERE` precede `WHERE`.

Se a configuração [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) estiver definida como 0, as heurísticas para mover automaticamente partes de expressões de `WHERE` para `PREWHERE` serão desativadas.

Se a consulta tiver o modificador [FINAL](/pt-BR/sql-reference/statements/select/from#final-modifier), a otimização de `PREWHERE` nem sempre estará correta. Ela é habilitada somente se ambas as configurações [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) e [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) estiverem ativadas.

:::note
A seção `PREWHERE` é executada antes de `FINAL`, portanto os resultados das consultas `FROM ... FINAL` podem ficar distorcidos ao usar `PREWHERE` com campos que não estão na seção `ORDER BY` de uma tabela.
:::

<div id="limitations">
  ## Limitações
</div>

`PREWHERE` é suportado apenas por tabelas da família [*MergeTree](../../../engines/table-engines/mergetree-family/index.md).

<div id="example">
  ## Exemplo
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```