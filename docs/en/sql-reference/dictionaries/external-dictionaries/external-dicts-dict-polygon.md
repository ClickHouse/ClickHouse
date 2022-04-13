---
toc_priority: 46
toc_title: Polygon Dictionaries With Grids
---

# Polygon dictionaries {#polygon-dictionaries}

Polygon dictionaries allow you to efficiently search for the polygon containing specified points.
For example: defining a city area by geographical coordinates.

Example of a polygon dictionary configuration:

``` xml
<dictionary>
    <structure>
        <key>
            <name>key</name>
            <type>Array(Array(Array(Array(Float64))))</type>
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

The corresponding [DDL-query](../../../sql-reference/statements/create/dictionary.md#create-dictionary-query):
``` sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
...
```

When configuring the polygon dictionary, the key must have one of two types:

-   A simple polygon. It is an array of points.
-   MultiPolygon. It is an array of polygons. Each polygon is a two-dimensional array of points. The first element of this array is the outer boundary of the polygon, and subsequent elements specify areas to be excluded from it.

Points can be specified as an array or a tuple of their coordinates. In the current implementation, only two-dimensional points are supported.

The user can [upload their own data](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) in all formats supported by ClickHouse.

There are 3 types of [in-memory storage](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) available:

-   `POLYGON_SIMPLE`. This is a naive implementation, where a linear pass through all polygons is made for each query, and membership is checked for each one without using additional indexes.

-   `POLYGON_INDEX_EACH`. A separate index is built for each polygon, which allows you to quickly check whether it belongs in most cases (optimized for geographical regions).
Also, a grid is superimposed on the area under consideration, which significantly narrows the number of polygons under consideration.
The grid is created by recursively dividing the cell into 16 equal parts and is configured with two parameters.
The division stops when the recursion depth reaches `MAX_DEPTH` or when the cell crosses no more than `MIN_INTERSECTIONS` polygons.
To respond to the query, there is a corresponding cell, and the index for the polygons stored in it is accessed alternately.

-   `POLYGON_INDEX_CELL`. This placement also creates the grid described above. The same options are available. For each sheet cell, an index is built on all pieces of polygons that fall into it, which allows you to quickly respond to a request.

-   `POLYGON`. Synonym to `POLYGON_INDEX_CELL`.

Dictionary queries are carried out using standard [functions](../../../sql-reference/functions/ext-dict-functions.md) for working with external dictionaries.
An important difference is that here the keys will be the points for which you want to find the polygon containing them.

**Example**

Example of working with the dictionary defined above:

``` sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

As a result of executing the last command for each point in the 'points' table, a minimum area polygon containing this point will be found, and the requested attributes will be output.

**Example**

You can read columns from polygon dictionaries via SELECT query, just turn on the `store_polygon_key_column = 1` in the dictionary configuration or corresponding DDL-query.

Query:

``` sql
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = TinyLog;

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

Result:

``` text
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```
