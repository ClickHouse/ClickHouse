---
description: 'Documentation sur la modification des paramètres de table'
sidebar_label: 'SETTING'
sidebar_position: 38
slug: /sql-reference/statements/alter/setting
title: 'Modification des paramètres de table'
doc_type: 'référence'
---

Il existe un ensemble de requêtes permettant de modifier les paramètres de table. Vous pouvez modifier des paramètres ou les rétablir à leurs valeurs par défaut. Une même requête peut modifier plusieurs paramètres à la fois.
Si aucun paramètre ne correspond au nom indiqué, la requête renvoie une exception.

**Syntaxe**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
Ces requêtes ne peuvent être appliquées qu’aux tables [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).
:::

<div id="modify-setting">
  ## MODIFY SETTING
</div>

Modifie les paramètres de la table.

**Syntaxe**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Exemple**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

<div id="reset-setting">
  ## RESET SETTING
</div>

Réinitialise les paramètres de la table à leurs valeurs par défaut. Si un paramètre est déjà dans son état par défaut, aucune action n’est effectuée.

**Syntaxe**

```sql
RESET SETTING setting_name [, ...]
```

**Exemple**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**Voir aussi**

* [Paramètres de MergeTree](../../../operations/settings/merge-tree-settings.md)