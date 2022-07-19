---
sidebar_position: 38
sidebar_label: SETTING
---

# Изменение настроек таблицы {#table_settings_manipulations}

Существуют запросы, которые изменяют настройки таблицы или сбрасывают их в значения по умолчанию. В одном запросе можно изменить сразу несколько настроек.
Если настройка с указанным именем не существует, то генерируется исключение.

**Синтаксис**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

    :::note "Примечание"
    Эти запросы могут применяться только к таблицам на движке [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).
    :::

## MODIFY SETTING {#alter_modify_setting}

Изменяет настройки таблицы.

**Синтаксис**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**Пример**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

## RESET SETTING {#alter_reset_setting}

Сбрасывает настройки таблицы в значения по умолчанию. Если настройка уже находится в состоянии по умолчанию, то никакие действия не выполняются.

**Синтаксис**

```sql
RESET SETTING setting_name [, ...]
```

**Пример**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**Смотрите также**

-   [Настройки MergeTree таблиц](../../../operations/settings/merge-tree-settings.md)
