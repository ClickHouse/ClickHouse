---
description: 'Documentação sobre manipulação de índices para omissão de dados'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'Manipulação de índices para omissão de dados'
toc_hidden_folder: true
doc_type: 'reference'
---

As operações a seguir estão disponíveis:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - Adiciona a descrição do índice aos metadados das tabelas.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - Remove a descrição do índice dos metadados da tabela e exclui os arquivos de índice do disco. É implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Reconstrói o índice secundário `name` para a partição `partition_name` especificada. Implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations). Se a parte `IN PARTITION` for omitida, ele reconstrói o índice para todos os dados da tabela.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Exclui os arquivos de índice secundário do disco sem remover a descrição. É implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

Os comandos `ADD`, `DROP` e `CLEAR` são leves no sentido de que apenas alteram os metadados ou removem arquivos.
Além disso, eles são replicados, sincronizando os metadados dos índices via ClickHouse Keeper ou ZooKeeper.

:::note
A manipulação de índices é compatível apenas com tabelas com motor [`*MergeTree`](/pt-BR/engines/table-engines/mergetree-family/mergetree.md) (incluindo variantes [replicadas](/pt-BR/engines/table-engines/mergetree-family/replication.md)).
:::