---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u968E\u5C64\u8F9E\u66F8"
---

# 階層辞書 {#hierarchical-dictionaries}

ClickHouseは、階層辞書をサポートしています [数値キー](external-dicts-dict-structure.md#ext_dict-numeric-key).

次の階層構造を見てください:

``` text
0 (Common parent)
│
├── 1 (Russia)
│   │
│   └── 2 (Moscow)
│       │
│       └── 3 (Center)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

この階層は、次の辞書テーブルとして表すことができます。

| region_id | parent_region | region_name |
|------------|----------------|--------------|
| 1          | 0              | ロシア       |
| 2          | 1              | モスクワ     |
| 3          | 2              | 中央         |
| 4          | 0              | イギリス     |
| 5          | 4              | ロンドン     |

このテーブル列 `parent_region` これには、要素の最も近い親のキーが含まれます。

クリックハウスは [階層](external-dicts-dict-structure.md#hierarchical-dict-attr) プロパティ [外部辞書](index.md) 属性。 このプロパティを使用すると、上記のような階層辞書を構成できます。

その [dictGetHierarchy](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) 関数を使用すると、要素の親チェーンを取得することができます。

この例では、dictionaryの構造は次のようになります:

``` xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
