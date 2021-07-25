# RESET SETTING {#alter_reset_setting}

This query resets table settings to their default values. Several settings may be reset in a single query. 
If a setting was set previously then it is removed from table's metadata. If a setting has default value already then no action is taken.
The query raises an exception if the setting does not exist.

!!! note "Note"
    `RESET` statement can be applied only to [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) tables.

**Syntax**

```sql
ALTER TABLE [db.]table RESET SETTING setting_name [, ...];
```

**Example**

```sql
CREATE TABLE example_table (id UInt32, data String) 
    ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;
    
ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**See Also** 

-   [MergeTree settings](../../../operations/settings/merge-tree-settings.md)
