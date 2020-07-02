---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "Harici bir s\xF6zl\xFCk yap\u0131land\u0131rma"
---

# Harici bir sözlük yapılandırma {#dicts-external-dicts-dict}

Sözlük xml dosyası kullanılarak yapılandırılmışsa, sözlük yapılandırması aşağıdaki yapıya sahiptir:

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

İlgili [DDL-sorgu](../../statements/create.md#create-dictionary-query) aşağıdaki yapıya sahiptir:

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
-   [kaynaklı](external-dicts-dict-sources.md) — Source of the dictionary.
-   [düzen](external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [yapılı](external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [ömür](external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
