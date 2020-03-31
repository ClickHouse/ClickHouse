---
machine_translated: true
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

Correspondant [DDL-requête](../create.md#create-dictionary-query) a la structure suivante:

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
-   [source](external_dicts_dict_sources.md) — Source of the dictionary.
-   [disposition](external_dicts_dict_layout.md) — Dictionary layout in memory.
-   [structure](external_dicts_dict_structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [vie](external_dicts_dict_lifetime.md) — Frequency of dictionary updates.

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
