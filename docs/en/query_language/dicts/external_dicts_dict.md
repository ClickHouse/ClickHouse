# Configuring an External Dictionary {#dicts-external_dicts_dict}

The dictionary configuration has the following structure:

```xml
<dictionary>
    <name>dict_name</name>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>

    <allow_databases>
      <database>first_database</database>
      <database>second_database</database>
      <!-- [...] -->
    </allow_databases>

</dictionary>
```

- name – The identifier that can be used to access the dictionary. Use the characters `[a-zA-Z0-9_\-]`.
- [source](external_dicts_dict_sources.md) — Source of the dictionary.
- [layout](external_dicts_dict_layout.md) — Dictionary layout in memory.
- [structure](external_dicts_dict_structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
- [lifetime](external_dicts_dict_lifetime.md) — Frequency of dictionary updates.
- allow_databases – Database identifiers who can be used to let access the dictionary on a particular database. Could be optional, with the dictionary having a global scope.

[Original article](https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
