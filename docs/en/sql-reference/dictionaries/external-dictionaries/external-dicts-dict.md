---
toc_priority: 40
toc_title: Configuring an External Dictionary
---

# Configuring an External Dictionary {#dicts-external-dicts-dict}

If dictionary is configured using xml file, than dictionary configuration has the following structure:

``` xml
<dictionary>
    <name>dict_name</name>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>
</dictionary>
```

Corresponding [DDL-query](../../../sql-reference/statements/create/dictionary.md) has the following structure:

``` sql
CREATE DICTIONARY dict_name
(
    ... -- attributes
)
PRIMARY KEY ... -- complex or single key configuration
SOURCE(...) -- Source configuration
LAYOUT(...) -- Memory layout configuration
LIFETIME(...) -- Lifetime of dictionary in memory
```

-   `name` – The identifier that can be used to access the dictionary. Use the characters `[a-zA-Z0-9_\-]`.
-   [source](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) — Source of the dictionary.
-   [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [structure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [lifetime](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[Original article](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
