---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u5916\u90E8\u30C7\u30A3\u30AF\u30B7\u30E7\u30CA\u30EA\u306E\u69CB\u6210"
---

# 外部ディクショナリの構成 {#dicts-external-dicts-dict}

辞書がxmlファイルを使用して構成されている場合、辞書の構成は次の構造になります:

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

対応する [DDL-クエリ](../../statements/create.md#create-dictionary-query) 次の構造を持ちます:

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
-   [ソース](external-dicts-dict-sources.md) — Source of the dictionary.
-   [レイアウト](external-dicts-dict-layout.md) — Dictionary layout in memory.
-   [構造](external-dicts-dict-structure.md) — Structure of the dictionary . A key and attributes that can be retrieved by this key.
-   [生涯](external-dicts-dict-lifetime.md) — Frequency of dictionary updates.

[元の記事](https://clickhouse.com/docs/en/query_language/dicts/external_dicts_dict/) <!--hide-->
