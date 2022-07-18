---
sidebar_position: 38
sidebar_label: SETTING
---

# Table Settings Manipulations {#table_settings_manipulations}

There is a set of queries to change table settings. You can modify settings or reset them to default values. A single query can change several settings at once.
If a setting with the specified name does not exist, then the query raises an exception.

**Syntax**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note    
These queries can be applied to [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) tables only.
:::

## MODIFY SETTING {#alter_modify_setting}

Changes table settings.

**Syntax**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Example**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

## RESET SETTING {#alter_reset_setting}

Resets table settings to their default values. If a setting is in a default state, then no action is taken.

**Syntax**

```sql
RESET SETTING setting_name [, ...]
```

**Example**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**See Also**

-   [MergeTree settings](../../../operations/settings/merge-tree-settings.md)
