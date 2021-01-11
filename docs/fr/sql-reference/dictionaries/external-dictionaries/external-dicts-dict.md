---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: Configuration D'un dictionnaire externe
---

# Configuration D'un dictionnaire externe {#dicts-external-dicts-dict}

Si dictionary est configuré à l'aide d'un fichier xml, than dictionary configuration a la structure suivante:

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

Correspondant [DDL-requête](../../statements/create.md#create-dictionary-query) a la structure suivante:

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
-   [source](external-dicts-dict-sources.md) — Source of the dictionary.
-   [disposition](external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [structure](external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [vie](external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
