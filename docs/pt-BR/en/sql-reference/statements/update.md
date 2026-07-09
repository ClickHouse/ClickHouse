---
description: 'As atualizações leves simplificam o processo de atualização de dados no banco de dados usando partes de patch.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: 'A instrução Lightweight UPDATE'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
As atualizações leves estão em beta no momento.
Se você encontrar problemas, abra uma issue no [repositório do ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

A instrução `UPDATE` leve atualiza linhas em uma tabela `[db.]table` que correspondem à expressão `filter_expr`.
Ela é chamada de &quot;atualização leve&quot; para diferenciá-la da consulta [`ALTER TABLE ... UPDATE`](/pt-BR/sql-reference/statements/alter/update), que é um processo pesado que reescreve colunas inteiras nas partes de dados.
Ela está disponível apenas para a família de motores de tabela [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree).

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

O `filter_expr` deve ser do tipo `UInt8`. Esta consulta atualiza os valores das colunas especificadas para os valores das expressões correspondentes nas linhas em que o `filter_expr` assume um valor diferente de zero.
Os valores são convertidos para o tipo da coluna usando o operador `CAST`. Não há suporte para atualizar colunas usadas no cálculo da chave primária ou das chaves de partição.

<div id="examples">
  ## Exemplos
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## As atualizações leves não atualizam os dados imediatamente
</div>

O `UPDATE` leve é implementado usando **partes de patch** — um tipo especial de parte de dados que contém apenas as colunas e linhas atualizadas.
Um `UPDATE` leve cria partes de patch, mas não modifica fisicamente os dados originais no armazenamento de imediato.
O processo de atualização é semelhante a uma consulta `INSERT ... SELECT ...`, mas a consulta `UPDATE` só retorna depois que a criação da parte de patch é concluída.

Os valores atualizados ficam:

* **Imediatamente visíveis** em consultas `SELECT` por meio da aplicação de patches
* **Fisicamente materializados** apenas durante merges e mutações posteriores
* **Automaticamente removidos** assim que todas as partes ativas tiverem os patches materializados

<div id="lightweight-update-requirements">
  ## Requisitos para atualizações leves
</div>

Há suporte para atualizações leves nos motores [`MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/pt-BR/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/pt-BR/engines/table-engines/mergetree-family/collapsingmergetree), [`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree) e em suas versões [`Replicated`](/pt-BR/engines/table-engines/mergetree-family/replication.md) e [`Shared`](/pt-BR/cloud/reference/shared-merge-tree).

Para usar atualizações leves, a materialização das colunas `_block_number` e `_block_offset` deve estar habilitada por meio das configurações da tabela [`enable_block_number_column`](/pt-BR/operations/settings/merge-tree-settings#enable_block_number_column) e [`enable_block_offset_column`](/pt-BR/operations/settings/merge-tree-settings#enable_block_offset_column).

<div id="lightweight-delete">
  ## Exclusões leves
</div>

Uma consulta de [exclusão leve](/pt-BR/sql-reference/statements/delete) pode ser executada como uma atualização leve em vez de uma mutação `ALTER UPDATE`. A implementação de exclusão leve é controlada pela configuração [`lightweight_delete_mode`](/pt-BR/operations/settings/settings#lightweight_delete_mode).

<div id="performance-considerations">
  ## Considerações de desempenho
</div>

**Vantagens das atualizações leves:**

* A latência da atualização é comparável à latência da consulta `INSERT ... SELECT ...`
* Somente as colunas e os valores atualizados são gravados, não colunas inteiras nas partes de dados
* Não é necessário aguardar a conclusão de merges/mutações em execução no momento; portanto, a latência de uma atualização é previsível
* É possível executar atualizações leves em paralelo

**Possíveis impactos no desempenho:**

* Adiciona sobrecarga às consultas `SELECT` que precisam aplicar patches
* [Índices de data skipping](/pt-BR/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) não serão usados para colunas em partes de dados às quais patches precisem ser aplicados. [Projeções](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#projections) não serão usadas se houver partes de patch para a tabela, inclusive em partes de dados que não tenham patches a serem aplicados.
* Atualizações pequenas feitas com frequência excessiva podem levar ao erro &quot;too many parts&quot;. Recomenda-se agrupar várias atualizações em uma única consulta, por exemplo colocando os IDs a serem atualizados em uma única cláusula `IN` dentro da cláusula `WHERE`
* As atualizações leves foram projetadas para atualizar pequenas quantidades de linhas (até cerca de 10% da tabela). Se você precisar atualizar uma quantidade maior, recomenda-se usar a mutação [`ALTER TABLE ... UPDATE`](/pt-BR/sql-reference/statements/alter/update)

<div id="concurrent-operations">
  ## Operações concorrentes
</div>

Atualizações leves não esperam a conclusão de merges/mutações em execução, ao contrário das mutações pesadas.
A consistência de atualizações leves concorrentes é controlada pelas configurações [`update_sequential_consistency`](/pt-BR/operations/settings/settings#update_sequential_consistency) e [`update_parallel_mode`](/pt-BR/operations/settings/settings#update_parallel_mode).

<div id="update-permissions">
  ## Permissões para `UPDATE`
</div>

`UPDATE` requer o privilégio `ALTER UPDATE`. Para habilitar instruções `UPDATE` em uma tabela específica para um usuário, execute:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## Detalhes da implementação
</div>

As partes de patch são iguais às partes regulares, mas contêm apenas colunas atualizadas e várias colunas de sistema:

* `_part` - o nome da parte original
* `_part_offset` - o número da linha na parte original
* `_block_number` - o número do bloco da linha na parte original
* `_block_offset` - o deslocamento do bloco da linha na parte original
* `_data_version` - a versão dos dados atualizados (número do bloco alocado para a consulta `UPDATE`)

Em média, isso adiciona cerca de 40 bytes de sobrecarga (dados não comprimidos) por linha atualizada nas partes de patch.
As colunas de sistema ajudam a localizar as linhas na parte original que devem ser atualizadas.
As colunas de sistema estão relacionadas às [colunas virtuais](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) na parte original, que são adicionadas para leitura quando as partes de patch precisam ser aplicadas.
As partes de patch são ordenadas por `_part` e `_part_offset`.

As partes de patch pertencem a partições diferentes da parte original.
O ID da partição da parte de patch é `patch-<hash of column names in patch part>-<original_partition_id>`.
Portanto, partes de patch com colunas diferentes são armazenadas em partições diferentes.
Por exemplo, três atualizações `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>` e `SET x = 1, y = 1 WHERE <cond>` criarão três partes de patch em três partições diferentes.

As partes de patch podem ser mescladas entre si para reduzir a quantidade de patches aplicados em consultas `SELECT` e diminuir a sobrecarga. A mesclagem de partes de patch usa o algoritmo de merge [replacing](/pt-BR/engines/table-engines/mergetree-family/replacingmergetree), com `_data_version` como coluna de versão.
Portanto, as partes de patch sempre armazenam a versão mais recente de cada linha atualizada na parte.

Atualizações leves não esperam que merges e mutações em execução terminem e sempre usam um snapshot atual das partes de dados para executar uma atualização e produzir uma parte de patch.
Por isso, pode haver dois casos de aplicação de partes de patch.

Por exemplo, se lermos a parte `A`, precisamos aplicar a parte de patch `X`:

* se `X` contiver a própria parte `A`. Isso acontece se `A` não estava participando de um merge quando `UPDATE` foi executado.
* se `X` contiver as partes `B` e `C`, que são cobertas pela parte `A`. Isso acontece se houvesse um merge (`B`, `C`) -&gt; `A` em execução quando `UPDATE` foi executado.

Para esses dois casos, há respectivamente duas formas de aplicar partes de patch:

* Usando merge pelas colunas ordenadas `_part`, `_part_offset`.
* Usando join pelas colunas `_block_number`, `_block_offset`.

O modo join é mais lento e exige mais memória do que o modo merge, mas é usado com menos frequência.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* [`ALTER UPDATE`](/pt-BR/sql-reference/statements/alter/update) - Operações pesadas com `UPDATE`
* [exclusão leve](/pt-BR/sql-reference/statements/delete) - Operações de exclusão leve
* [`APPLY PATCHES`](/pt-BR/sql-reference/statements/alter/apply-patches) - Força a materialização física dos patches nas partes de dados (operação de mutação)