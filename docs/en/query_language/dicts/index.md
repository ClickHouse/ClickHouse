# Dictionaries

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.

ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

[NULL](../syntax.md#null-literal) values can't be stored in a dictionary.

ClickHouse supports:

- [Built-in dictionaries](internal_dicts.md#internal_dicts) with a specific [set of functions](../functions/ym_dict_functions.md#ym_dict_functions).
- [Plug-in (external) dictionaries](external_dicts.md#dicts-external_dicts) with a [set of functions](../functions/ext_dict_functions.md#ext_dict_functions).

