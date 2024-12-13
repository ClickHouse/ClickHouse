---
slug: /ja/sql-reference/statements/alter/setting
sidebar_position: 38
sidebar_label: SETTING
---

# テーブル設定の操作

テーブル設定を変更するための一連のクエリがあります。設定を変更したり、デフォルト値にリセットしたりできます。一つのクエリで複数の設定を一度に変更することが可能です。指定された名前の設定が存在しない場合、クエリは例外を発生させます。

**構文**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
これらのクエリは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) テーブルのみに適用されます。
:::

## MODIFY SETTING

テーブルの設定を変更します。

**構文**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**例**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

## RESET SETTING

テーブルの設定をデフォルト値にリセットします。設定がデフォルトの状態にある場合は、何も行われません。

**構文**

```sql
RESET SETTING setting_name [, ...]
```

**例**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**参照**

- [MergeTree settings](../../../operations/settings/merge-tree-settings.md)
