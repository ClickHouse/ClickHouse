---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_folder_title: "\u5B57\u5178"
toc_priority: 35
toc_title: "\u5BFC\u8A00"
---

# 字典 {#dictionaries}

字典是一个映射 (`key -> attributes`）这是方便各种类型的参考清单。

ClickHouse支持使用可用于查询的字典的特殊功能。 这是更容易和更有效地使用字典与功能比 `JOIN` 与参考表。

[NULL](../syntax.md#null) 值不能存储在字典中。

ClickHouse支持:

-   [内置字典](internal_dicts.md#internal_dicts) 具有特定的 [功能集](../../sql_reference/functions/ym_dict_functions.md).
-   [插件（外部）字典](external_dictionaries/external_dicts.md) 用一个 [职能净额](../../sql_reference/functions/ext_dict_functions.md).

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
