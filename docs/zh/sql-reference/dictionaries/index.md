---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u5B57\u5178"
toc_priority: 35
toc_title: "\u5BFC\u8A00"
---

# 字典 {#dictionaries}

字典是一个映射 (`键 -> 属性`）, 是方便各种类型的参考清单。

ClickHouse支持一些特殊函数配合字典在查询中使用。 将字典与函数结合使用比将 `JOIN` 操作与引用表结合使用更简单、更有效。

[NULL](../../sql-reference/syntax.md#null-literal) 值不能存储在字典中。

ClickHouse支持:

-   [内置字典](internal-dicts.md#internal_dicts) ,这些字典具有特定的 [函数集](../../sql-reference/functions/ym-dict-functions.md).
-   [插件（外部）字典](external-dictionaries/external-dicts.md#dicts-external-dicts) ,这些字典拥有一个 [函数集](../../sql-reference/functions/ext-dict-functions.md).

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
