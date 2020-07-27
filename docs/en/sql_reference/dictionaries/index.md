---
toc_folder_title: Dictionaries
toc_priority: 35
toc_title: Introduction
---

# Dictionaries {#dictionaries}

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.

ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

[NULL](../syntax.md#null) values can’t be stored in a dictionary.

ClickHouse supports:

-   [Built-in dictionaries](internal_dicts.md#internal_dicts) with a specific [set of functions](../../sql_reference/functions/ym_dict_functions.md).
-   [Plug-in (external) dictionaries](external_dictionaries/external_dicts.md) with a [net of functions](../../sql_reference/functions/ext_dict_functions.md).

[Original article](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
