---
description: 'Documentação sobre APPLY PATCHES de atualizações leves'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'APPLY PATCHES de atualizações leves'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

O comando aciona manualmente a materialização física das partes de patch criadas por instruções de [lightweight `UPDATE`](/pt-BR/sql-reference/statements/update). Ele aplica à força os patches pendentes às partes de dados, reescrevendo apenas as colunas afetadas.

:::note

* Ele só funciona para tabelas da família [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluindo tabelas [replicated](../../../engines/table-engines/mergetree-family/replication.md)).
* Esta é uma operação de mutação e é executada de forma assíncrona em segundo plano.
  :::

<div id="when-to-use">
  ## Quando usar APPLY PATCHES
</div>

:::tip
Em geral, você não deve precisar usar `APPLY PATCHES`
:::

As partes de patch normalmente são aplicadas automaticamente durante as mesclagens quando a configuração [`apply_patches_on_merge`](/pt-BR/operations/settings/merge-tree-settings#apply_patches_on_merge) está habilitada (padrão). No entanto, talvez você queira forçar manualmente a aplicação dos patches nestes cenários:

* Para reduzir a sobrecarga de aplicar patches durante consultas `SELECT`
* Para consolidar várias partes de patch antes que se acumulem
* Para preparar dados para backup ou exportação com os patches já materializados
* Quando `apply_patches_on_merge` estiver desabilitada e você quiser controlar quando os patches são aplicados

<div id="examples">
  ## Exemplos
</div>

Aplique todos os patches pendentes em uma tabela:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

Aplique patches somente a uma partição específica:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Combine com outras operações:

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## Monitoramento da aplicação do patch
</div>

Você pode acompanhar o progresso da aplicação do patch usando a tabela [`system.mutations`](/pt-BR/operations/system-tables/mutations):

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## Veja também
</div>

* [`UPDATE` leve](/pt-BR/sql-reference/statements/update) - Criar parte de patch com atualizações leves
* [configuração `apply_patches_on_merge`](/pt-BR/operations/settings/merge-tree-settings#apply_patches_on_merge) - Controlar a aplicação automática de patches durante mesclagens