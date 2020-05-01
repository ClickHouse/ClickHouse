---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: "\u968E\u5C64\u8F9E\u66F8"
---

# 階層辞書 {#hierarchical-dictionaries}

クリックハウスは、 [数値キー](external-dicts-dict-structure.md#ext_dict-numeric-key).

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

この階層として表現することができ、以下の辞書。

| region\_id | parent\_region | region\_name |
|------------|----------------|--------------|
| 1          | 0              | ロシア       |
| 2          | 1              | モスクワ     |
| 3          | 2              | 中央         |
| 4          | 0              | イギリス     |
| 5          | 4              | ロンドン     |

この表には列が含まれます `parent_region` これには、要素の最も近い親のキーが含まれます。

クリックハウスは [階層](external-dicts-dict-structure.md#hierarchical-dict-attr) のための特性 [外部辞書](index.md) 属性。 このプロパティを使用すると、上記のような階層辞書を構成できます。

その [独裁主義体制](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) 関数を使用すると、要素の親チェーンを取得できます。

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
