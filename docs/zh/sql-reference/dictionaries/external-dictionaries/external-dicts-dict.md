---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u914D\u7F6E\u5916\u90E8\u5B57\u5178"
---

# 配置外部字典 {#dicts-external-dicts-dict}

如果使用xml文件配置字典，则比字典配置具有以下结构:

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

相应的 [DDL-查询](../../statements/create.md#create-dictionary-query) 具有以下结构:

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
-   [来源](external-dicts-dict-sources.md) — Source of the dictionary.
-   [布局](external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [结构](external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [使用寿命](external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
