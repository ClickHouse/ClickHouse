---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u5206\u5C42\u5B57\u5178"
---

# 分层字典 {#hierarchical-dictionaries}

ClickHouse支持分层字典与 [数字键](external-dicts-dict-structure.md#ext_dict-numeric-key).

看看下面的层次结构:

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

这种层次结构可以表示为下面的字典表。

| region\_id | parent\_region | region\_name |
|------------|----------------|--------------|
| 1          | 0              | 俄罗斯       |
| 2          | 1              | 莫斯科       |
| 3          | 2              | 中心         |
| 4          | 0              | 英国         |
| 5          | 4              | 伦敦         |

此表包含一列 `parent_region` 包含该元素的最近父项的键。

ClickHouse支持 [等级](external-dicts-dict-structure.md#hierarchical-dict-attr) 属性为 [外部字典](index.md) 属性。 此属性允许您配置类似于上述的分层字典。

该 [独裁主义](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) 函数允许您获取元素的父链。

对于我们的例子，dictionary的结构可以是以下内容:

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

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
