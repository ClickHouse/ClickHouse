---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Polygon 字典'
sidebar_label: 'Polygon'
sidebar_position: 12
description: '配置用于点在多边形内查询的 Polygon 字典。'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`polygon` (`POLYGON`) 字典针对点是否位于多边形内的查询做了优化，本质上是一种“反向地理编码”查找。
给定一个坐标 (纬度/经度) ，它可以高效找出包含该点的多边形/区域 (从大量多边形组成的集合中，例如国家或区域边界) 。
它非常适合将位置坐标映射到其所在区域。

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="ClickHouse 中的 Polygon 字典" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

配置 polygon 字典的示例：

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
            </key>

            <attribute>
                <name>name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>

        <layout>
            <polygon>
                <store_polygon_key_column>1</store_polygon_key_column>
            </polygon>
        </layout>

        ...
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />

配置 polygon 字典时，键必须是以下两种类型之一：

* 简单多边形。它是一个点的数组。
* MultiPolygon。它是一个多边形数组。每个多边形都是一个二维点数组。该数组的第一个元素是多边形的外边界，后续元素指定要从中排除的区域。

点可以指定为坐标数组或坐标元组。在当前实现中，仅支持二维点。

用户可以使用 ClickHouse 支持的任意格式上传自己的数据。

可用的[内存中存储](./#storing-dictionaries-in-memory)共有 3 种类型：

| 布局                   | 描述                                                                                                                                                 |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | 朴素实现。每次查询都会线性遍历所有多边形，在没有额外索引的情况下检查该点是否位于其中。                                                                                                        |
| `POLYGON_INDEX_EACH` | 为每个多边形单独构建索引，因此在大多数情况下都能快速完成包含关系检查 (针对地理区域做了优化) 。系统会在区域上叠加网格，并将单元递归划分为 16 个相等的部分。当递归深度达到 `MAX_DEPTH`，或者某个单元穿过的多边形数量不超过 `MIN_INTERSECTIONS` 时，划分停止。 |
| `POLYGON_INDEX_CELL` | 也会使用与上述相同的选项创建前面描述的网格。对于每个叶子单元，都会基于落入其中的所有多边形片段构建索引，从而实现快速查询响应。                                                                                    |
| `POLYGON`            | `POLYGON_INDEX_CELL` 的同义词。                                                                                                                         |

字典查询通过用于处理字典的标准[函数](/zh/sql-reference/functions/ext-dict-functions.md)执行。
一个重要区别是，这里的键是你想要查找其所属多边形的那些点。

**示例**

使用上面定义的字典的示例：

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

对 `points` 表中的每个 Point 执行上一条命令后，将找到包含该 Point 的最小面积多边形，并输出请求的属性。

**示例**

您可以通过 SELECT 查询读取 Polygon 字典中的列，只需在字典配置或相应的 DDL 查询中开启 `store_polygon_key_column = 1` 即可。

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```