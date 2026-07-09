---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Polygon dictionaries'
sidebar_label: 'Polygon'
sidebar_position: 12
description: 'point-in-polygon ルックアップ用の Polygon dictionaries を設定します。'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`polygon` (`POLYGON`) Dictionary は、ポイントインポリゴンのクエリ、つまり実質的には「逆ジオコーディング」のルックアップ向けに最適化されています。
座標 (緯度/経度) が与えられると、その点を含むポリゴン/リージョン (国や地域の境界など、多数のポリゴンの集合から) を効率的に特定します。
位置座標を、その座標を含むリージョンに対応付ける用途に適しています。

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="ClickHouse の Polygon Dictionaries" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

polygon Dictionary の設定例:

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

  <TabItem value="xml" label="設定ファイル">
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

polygon Dictionary を設定する際、キーは次の 2 つの型のいずれかである必要があります:

* 単純なポリゴン。points の配列です。
* MultiPolygon。polygons の配列です。各 polygon は points の 2 次元配列です。この配列の最初の要素は polygon の外側境界で、後続の要素はそこから除外する領域を指定します。

points は、座標の配列または Tuple として指定できます。現在の実装では、2 次元の points のみがサポートされています。

ユーザーは、ClickHouse がサポートするすべてのフォーマットで独自のデータをアップロードできます。

利用可能な [in-memory storage](./#storing-dictionaries-in-memory) には 3 つのタイプがあります:

| Layout               | 説明                                                                                                                                                                                |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | 単純な実装です。追加の索引を使わず、各クエリごとにすべてのポリゴンを線形走査して包含判定を行います。                                                                                                                                |
| `POLYGON_INDEX_EACH` | 各ポリゴンごとに個別の索引を構築し、多くの場合で高速な包含判定を可能にします (地理的リージョン向けに最適化) 。領域上にグリッドを重ね、cell を再帰的に 16 個の等しい部分に分割します。再帰の深さが `MAX_DEPTH` に達するか、ある cell が交差するポリゴン数が `MIN_INTERSECTIONS` 以下になると分割は停止します。 |
| `POLYGON_INDEX_CELL` | 上記と同じオプションで、同様のグリッドも作成します。各リーフ cell について、その中に含まれるすべてのポリゴン片に対する索引を構築し、高速なクエリ応答を可能にします。                                                                                             |
| `POLYGON`            | `POLYGON_INDEX_CELL` の同義語です。                                                                                                                                                      |

Dictionary のクエリは、Dictionary を扱うための標準的な [Functions](/ja/sql-reference/functions/ext-dict-functions.md) を使用して実行します。
重要な違いは、ここではキーが、その点を含むポリゴンを見つけたい points になることです。

**例**

上で定義した Dictionary を使用する例:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

「points」テーブル内の各 Point に対して最後のコマンドを実行すると、その Point を含む最小面積の Polygon が求められ、指定した属性が出力されます。

**例**

SELECTクエリを使って polygon dictionaries のカラムを読み取れます。Dictionary の設定または対応する DDL クエリで `store_polygon_key_column = 1` を有効にするだけです。

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