---
slug: /en/sql-reference/functions/ext-dict-functions
sidebar_position: 50
sidebar_label: Dictionaries
---

# Functions for Working with Dictionaries

:::note
For dictionaries created with [DDL queries](../../sql-reference/statements/create/dictionary.md), the `dict_name` parameter must be fully specified, like `<database>.<dict_name>`. Otherwise, the current database is used.
:::

For information on connecting and configuring dictionaries, see [Dictionaries](../../sql-reference/dictionaries/index.md).

## dictGet, dictGetOrDefault, dictGetOrNull

Retrieves values from a dictionary.

``` sql
dictGet('dict_name', attr_names, id_expr)
dictGetOrDefault('dict_name', attr_names, id_expr, default_value_expr)
dictGetOrNull('dict_name', attr_name, id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../data-types/tuple.md)-type value depending on the dictionary configuration.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

- If ClickHouse parses the attribute successfully in the [attribute’s data type](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes), functions return the value of the dictionary attribute that corresponds to `id_expr`.

- If there is no the key, corresponding to `id_expr`, in the dictionary, then:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.
        - `dictGetOrNull` returns `NULL` in case key was not found in dictionary.

ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.

**Example for simple key dictionary**

Create a text file `ext-dict-test.csv` containing the following:

``` text
1,1
2,2
```

The first column is `id`, the second column is `c1`.

Configure the dictionary:

``` xml
<clickhouse>
    <dictionary>
        <name>ext-dict-test</name>
        <source>
            <file>
                <path>/path-to/ext-dict-test.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</clickhouse>
```

Perform the query:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Example for complex key dictionary**

Create a text file `ext-dict-mult.csv` containing the following:

``` text
1,1,'1'
2,2,'2'
3,3,'3'
```

The first column is `id`, the second is `c1`, the third is `c2`.

Configure the dictionary:

``` xml
<clickhouse>
    <dictionary>
        <name>ext-dict-mult</name>
        <source>
            <file>
                <path>/path-to/ext-dict-mult.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
            <attribute>
                <name>c2</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</clickhouse>
```

Perform the query:

``` sql
SELECT
    dictGet('ext-dict-mult', ('c1','c2'), number + 1) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

``` text
┌─val─────┬─type──────────────────┐
│ (1,'1') │ Tuple(UInt8, String)  │
│ (2,'2') │ Tuple(UInt8, String)  │
│ (3,'3') │ Tuple(UInt8, String)  │
└─────────┴───────────────────────┘
```

**Example for range key dictionary**

Input table:

```sql
CREATE TABLE range_key_dictionary_source_table
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
ENGINE = TinyLog();

INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
```

Create the dictionary:

```sql
CREATE DICTIONARY range_key_dictionary
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);
```

Perform the query:

``` sql
SELECT
    (number, toDate('2019-05-20')),
    dictHas('range_key_dictionary', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value_nullable', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', ('value', 'value_nullable'), number, toDate('2019-05-20'))
FROM system.numbers LIMIT 5 FORMAT TabSeparated;
```
Result:

``` text
(0,'2019-05-20')        0       \N      \N      (NULL,NULL)
(1,'2019-05-20')        1       First   First   ('First','First')
(2,'2019-05-20')        1       Second  \N      ('Second',NULL)
(3,'2019-05-20')        1       Third   Third   ('Third','Third')
(4,'2019-05-20')        0       \N      \N      (NULL,NULL)
```

**See Also**

- [Dictionaries](../../sql-reference/dictionaries/index.md)

## dictHas

Checks whether a key is present in a dictionary.

``` sql
dictHas('dict_name', id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../data-types/tuple.md)-type value depending on the dictionary configuration.

**Returned value**

- 0, if there is no key. [UInt8](../data-types/int-uint.md).
- 1, if there is a key. [UInt8](../data-types/int-uint.md).

## dictGetHierarchy

Creates an array, containing all the parents of a key in the [hierarchical dictionary](../../sql-reference/dictionaries/index.md#hierarchical-dictionaries).

**Syntax**

``` sql
dictGetHierarchy('dict_name', key)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md)-type value.

**Returned value**

- Parents for the key. [Array(UInt64)](../data-types/array.md).

## dictIsIn

Checks the ancestor of a key through the whole hierarchical chain in the dictionary.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `child_id_expr` — Key to be checked. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md)-type value.
- `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md)-type value.

**Returned value**

- 0, if `child_id_expr` is not a child of `ancestor_id_expr`. [UInt8](../data-types/int-uint.md).
- 1, if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`. [UInt8](../data-types/int-uint.md).

## dictGetChildren

Returns first-level children as an array of indexes. It is the inverse transformation for [dictGetHierarchy](#dictgethierarchy).

**Syntax**

``` sql
dictGetChildren(dict_name, key)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md)-type value.

**Returned values**

- First-level descendants for the key. [Array](../data-types/array.md)([UInt64](../data-types/int-uint.md)).

**Example**

Consider the hierarchic dictionary:

``` text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```

First-level children:

``` sql
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetChildren('hierarchy_flat_dictionary', number)─┐
│ [1]                                                  │
│ [2,3]                                                │
│ [4]                                                  │
│ []                                                   │
└──────────────────────────────────────────────────────┘
```

## dictGetDescendant

Returns all descendants as if [dictGetChildren](#dictgetchildren) function was applied `level` times recursively.

**Syntax**

``` sql
dictGetDescendants(dict_name, key, level)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md)-type value.
- `level` — Hierarchy level. If `level = 0` returns all descendants to the end. [UInt8](../data-types/int-uint.md).

**Returned values**

- Descendants for the key. [Array](../data-types/array.md)([UInt64](../data-types/int-uint.md)).

**Example**

Consider the hierarchic dictionary:

``` text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```
All descendants:

``` sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetDescendants('hierarchy_flat_dictionary', number)─┐
│ [1,2,3,4]                                               │
│ [2,3,4]                                                 │
│ [4]                                                     │
│ []                                                      │
└─────────────────────────────────────────────────────────┘
```

First-level descendants:

``` sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetDescendants('hierarchy_flat_dictionary', number, 1)─┐
│ [1]                                                        │
│ [2,3]                                                      │
│ [4]                                                        │
│ []                                                         │
└────────────────────────────────────────────────────────────┘
```


## dictGetAll

Retrieves the attribute values of all nodes that matched each key in a [regular expression tree dictionary](../../sql-reference/dictionaries/index.md#regexp-tree-dictionary).

Besides returning values of type `Array(T)` instead of `T`, this function behaves similarly to [`dictGet`](#dictget-dictgetordefault-dictgetornull).

**Syntax**

``` sql
dictGetAll('dict_name', attr_names, id_expr[, limit])
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning array of dictionary key-type value or [Tuple](../data-types/tuple.md)-type value depending on the dictionary configuration.
- `limit` - Maximum length for each value array returned. When truncating, child nodes are given precedence over parent nodes, and otherwise the defined list order for the regexp tree dictionary is respected. If unspecified, array length is unlimited.

**Returned value**

- If ClickHouse parses the attribute successfully in the attribute’s data type as defined in the dictionary, returns an array of dictionary attribute values that correspond to `id_expr` for each attribute specified by `attr_names`.

- If there is no key corresponding to `id_expr` in the dictionary, then an empty array is returned.

ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.

**Example**

Consider the following regexp tree dictionary:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'foo'
  tag: 'foo_attr'
- regexp: 'bar'
  tag: 'bar_attr'
- regexp: 'baz'
  tag: 'baz_attr'
```

Get all matching values:

```sql
SELECT dictGetAll('regexp_dict', 'tag', 'foobarbaz');
```

```text
┌─dictGetAll('regexp_dict', 'tag', 'foobarbaz')─┐
│ ['foo_attr','bar_attr','baz_attr']            │
└───────────────────────────────────────────────┘
```

Get up to 2 matching values:

```sql
SELECT dictGetAll('regexp_dict', 'tag', 'foobarbaz', 2);
```

```text
┌─dictGetAll('regexp_dict', 'tag', 'foobarbaz', 2)─┐
│ ['foo_attr','bar_attr']                          │
└──────────────────────────────────────────────────┘
```

## Other Functions

ClickHouse supports specialized functions that convert dictionary attribute values to a specific data type regardless of the dictionary configuration.

Functions:

- `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
- `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
- `dictGetFloat32`, `dictGetFloat64`
- `dictGetDate`
- `dictGetDateTime`
- `dictGetUUID`
- `dictGetString`
- `dictGetIPv4`, `dictGetIPv6`

All these functions have the `OrDefault` modification. For example, `dictGetDateOrDefault`.

Syntax:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_name` — Name of the column of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../data-types/int-uint.md) or [Tuple](../data-types/tuple.md)-type value depending on the dictionary configuration.
- `default_value_expr` — Value returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning the value in the data type configured for the `attr_name` attribute.

**Returned value**

- If ClickHouse parses the attribute successfully in the [attribute’s data type](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes), functions return the value of the dictionary attribute that corresponds to `id_expr`.

- If there is no requested `id_expr` in the dictionary then:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.
