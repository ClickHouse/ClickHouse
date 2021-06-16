---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u067E\u06CC\u06A9\u0631\u0628\u0646\u062F\u06CC \u06CC\u06A9 \u0641\u0631\
  \u0647\u0646\u06AF \u0644\u063A\u062A \u062E\u0627\u0631\u062C\u06CC"
---

# پیکربندی یک فرهنگ لغت خارجی {#dicts-external-dicts-dict}

اگر فرهنگ لغت با استفاده از فایل میلی لیتر پیکربندی, از پیکربندی فرهنگ لغت دارای ساختار زیر:

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

متناظر [توصیف](../../statements/create.md#create-dictionary-query) دارای ساختار زیر است:

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
-   [متن](external-dicts-dict-sources.md) — Source of the dictionary.
-   [طرحبندی](external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [ساختار](external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [طول عمر](external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
