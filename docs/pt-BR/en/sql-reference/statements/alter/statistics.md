---
description: 'Documentação sobre manipulação de estatísticas de colunas'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'Manipulação de estatísticas de colunas'
doc_type: 'referência'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # Manipulação de estatísticas de colunas
</div>

<CloudNotSupportedBadge />

As operações a seguir estão disponíveis:

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Adiciona a descrição das estatísticas aos metadados das tabelas.

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Modifica a descrição das estatísticas nos metadados das tabelas.

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Remove as estatísticas dos metadados das colunas especificadas e exclui todos os objetos de estatísticas em todas as partes dessas colunas.

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Exclui todos os objetos de estatísticas em todas as partes das colunas especificadas. Os objetos de estatísticas podem ser recriados usando `ALTER TABLE MATERIALIZE STATISTICS`.

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - Recria as estatísticas das colunas. Implementado como uma [mutação](../../../sql-reference/statements/alter/index.md#mutations).

Os dois primeiros comandos são leves, no sentido de que apenas alteram metadados ou removem arquivos.

Além disso, esses comandos são replicados, sincronizando os metadados das estatísticas via ZooKeeper.

<div id="example">
  ## Exemplo:
</div>

Adicionando dois tipos de estatística a duas colunas:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
As estatísticas são suportadas apenas por tabelas com o motor [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluindo variantes [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::