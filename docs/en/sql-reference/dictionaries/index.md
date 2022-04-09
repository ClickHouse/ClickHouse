---
sidebar_label: Dictionaries
sidebar_position: 35
---

# Dictionaries {#dictionaries}

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.

ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

ClickHouse supports:

-   [Built-in dictionaries](../../sql-reference/dictionaries/internal-dicts.md#internal_dicts) with a specific [set of functions](../../sql-reference/functions/ym-dict-functions.md).
-   [Plug-in (external) dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md#dicts-external-dicts) with a [set of functions](../../sql-reference/functions/ext-dict-functions.md).

