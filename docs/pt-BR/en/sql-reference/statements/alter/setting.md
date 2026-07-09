---
description: 'Documentação sobre modificações nas configurações de tabela'
sidebar_label: 'SETTING'
sidebar_position: 38
slug: /sql-reference/statements/alter/setting
title: 'Modificações nas configurações de tabela'
doc_type: 'reference'
---

Há um conjunto de consultas para alterar as configurações da tabela. Você pode modificar as configurações ou redefini-las para os valores padrão. Uma única consulta pode alterar várias configurações ao mesmo tempo.
Se uma configuração com o nome especificado não existir, a consulta gerará uma exceção.

**Sintaxe**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
Estas consultas podem ser aplicadas somente a tabelas [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).
:::

<div id="modify-setting">
  ## MODIFY SETTING
</div>

Altera as configurações da tabela.

**Sintaxe**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Exemplo**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

<div id="reset-setting">
  ## RESET SETTING
</div>

Restaura as configurações da tabela aos valores padrão. Se uma configuração já estiver no estado padrão, nenhuma ação será realizada.

**Sintaxe**

```sql
RESET SETTING setting_name [, ...]
```

**Exemplo**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**Veja também**

* [Configurações do MergeTree](../../../operations/settings/merge-tree-settings.md)