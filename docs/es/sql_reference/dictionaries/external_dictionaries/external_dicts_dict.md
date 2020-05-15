---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 40
toc_title: "Configuraci\xF3n de un diccionario externo"
---

# Configuración de un diccionario externo {#dicts-external-dicts-dict}

Si el diccionario se configura usando un archivo xml, la configuración del diccionario tiene la siguiente estructura:

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

Correspondiente [Consulta DDL](../../statements/create.md#create-dictionary-query) tiene la siguiente estructura:

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
-   [fuente](external_dicts_dict_sources.md) — Source of the dictionary.
-   [diseño](external_dicts_dict_layout.md) — Dictionary layout in memory.
-   [estructura](external_dicts_dict_structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [vida](external_dicts_dict_lifetime.md) — Frequency of dictionary updates.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
