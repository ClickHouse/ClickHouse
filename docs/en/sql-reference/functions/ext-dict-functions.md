---
slug: /en/sql-reference/functions/ext-dict-functions
sidebar_position: 50
sidebar_label: Dictionaries
---

# Functions for Working with Dictionaries

:::note
For dictionaries created with [DDL queries](../../sql-reference/statements/create/dictionary.md), the `dict_name` parameter must be fully specified: `<database>.<dict_name>`. Otherwise, the current database is used.
:::

For information on connecting and configuring dictionaries, see [Dictionaries](../../sql-reference/dictionaries/index.md).

## dictGet

Retrieves values from a dictionary.

**Syntax**

```sql
dictGet('dict_name', attr_names, id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../../sql-reference/data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../../sql-reference/data-types/tuple.md)-type value depending on the dictionary configuration.

**Returned value**

If ClickHouse parses the attribute successfully in the [attribute’s data type](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes), this function returns the value of the dictionary attribute that corresponds to `id_expr`.

If there is no the key, corresponding to `id_expr`, in the dictionary, then this function returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.

**Implementation details**

ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.

**Example**

Query:

```sql
CREATE TABLE source_table
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO source_table VALUES (1, 'First'), (2, 'Second');

CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LIFETIME(MIN  0 MAX  1000)
LAYOUT(FLAT());

SELECT dictGet('id_value_dictionary', 'value', toUInt64(1)) AS value;
```

**Result**:

```response
'First'
```

## dictGetOrDefault

Retrieves values from a dictionary. Returns the default value set by the user for keys that are not found in the dictionary.

**Syntax**

```sql
dictGetOrDefault('dict_name', attr_names, id_expr, default_value_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../../sql-reference/data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../../sql-reference/data-types/tuple.md)-type value depending on the dictionary configuration.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

If ClickHouse parses the attribute successfully in the [attribute’s data type](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes), this function returns the value of the dictionary attribute that corresponds to `id_expr`.

If there is no the key, corresponding to `id_expr`, in the dictionary, then returns the value passed as the `default_value_expr` parameter.

**Implementation details**

None.

**Example**

Query:

```sql
CREATE TABLE source_table
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO source_table VALUES (1, 'First'), (2, 'Second');

CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LIFETIME(MIN  0 MAX  1000)
LAYOUT(FLAT());

SELECT dictGetOrDefault('id_value_dictionary', 'value', toUInt64(1), 'Default return string.') AS value;
```

**Result**:

```response
'Default return string.'
```

## dictGetOrNull

Retrieves values from a dictionary. If that value cannot be found, returns null. This is different to `dictGet` which throws an exception if the value cannot be found.

**Syntax**

```sql
dictGetOrNull('dict_name', attr_names, id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../../sql-reference/data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../../sql-reference/data-types/tuple.md)-type value depending on the dictionary configuration.

**Returned value**

If ClickHouse parses the attribute successfully in the [attribute’s data type](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes), this function returns the value of the dictionary attribute that corresponds to `id_expr`.

If there is no the key, corresponding to `id_expr`, in the dictionary, then this fuction returns `null`.

**Implementation details**

None.

**Example**

Query:

```sql
CREATE TABLE source_table
(
    id UInt64,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO source_table VALUES (1, 'First'), (2, 'Second');

CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LIFETIME(MIN  0 MAX  1000)
LAYOUT(FLAT());

SELECT dictGetOrNull('id_value_dictionary', 'missing_attribute', toUInt64(1)) AS value;
```

**Result**:

```response
null
```

## dictHas

Checks whether a key is present in a dictionary.

``` sql
dictHas('dict_name', id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning dictionary key-type value or [Tuple](../../sql-reference/data-types/tuple.md)-type value depending on the dictionary configuration.

**Returned value**

- 0, if there is no key.
- 1, if there is a key.

Type: `UInt8`.

## dictGetHierarchy

Creates an array, containing all the parents of a key in the [hierarchical dictionary](../../sql-reference/dictionaries/index.md#hierarchical-dictionaries).

**Syntax**

``` sql
dictGetHierarchy('dict_name', key)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../../sql-reference/data-types/int-uint.md)-type value.

**Returned value**

- Parents for the key.

Type: [Array(UInt64)](../../sql-reference/data-types/array.md).

## dictIsIn

Checks the ancestor of a key through the whole hierarchical chain in the dictionary.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `child_id_expr` — Key to be checked. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../../sql-reference/data-types/int-uint.md)-type value.
- `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../../sql-reference/data-types/int-uint.md)-type value.

**Returned value**

- 0, if `child_id_expr` is not a child of `ancestor_id_expr`.
- 1, if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`.

Type: `UInt8`.

## dictGetChildren

Returns first-level children as an array of indexes. It is the inverse transformation for [dictGetHierarchy](#dictgethierarchy).

**Syntax**

``` sql
dictGetChildren(dict_name, key)
```

**Arguments**

- `dict_name` — Name of the dictionary. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../../sql-reference/data-types/int-uint.md)-type value.

**Returned values**

- First-level descendants for the key.

Type: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

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
- `key` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a [UInt64](../../sql-reference/data-types/int-uint.md)-type value.
- `level` — Hierarchy level. If `level = 0` returns all descendants to the end. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned values**

- Descendants for the key.

Type: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

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
- `attr_names` — Name of the column of the dictionary, [String literal](../../sql-reference/syntax.md#syntax-string-literal), or tuple of column names, [Tuple](../../sql-reference/data-types/tuple.md)([String literal](../../sql-reference/syntax.md#syntax-string-literal)).
- `id_expr` — Key value. [Expression](../../sql-reference/syntax.md#syntax-expressions) returning array of dictionary key-type value or [Tuple](../../sql-reference/data-types/tuple.md)-type value depending on the dictionary configuration.
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

## dictGetInt8

Retrieves type Int8 values from a dictionary.

**Syntax**

```sql
dictGetInt8('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int8` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int8
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Mali', 32);
INSERT INTO countries VALUES (2, 'Czechia', 28);
INSERT INTO countries VALUES (3, 'Brazil', 82);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int8
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt8('country_codes', 'code_designation', '3') AS code_designation_output;
```

Result:

```response
28
```

## dictGetInt8OrDefault

Retrieves type Int8 values from a dictionary. If the Int8 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetInt8OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int8` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int8
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Mali', 32);
INSERT INTO countries VALUES (2, 'Czechia', 28);
INSERT INTO countries VALUES (3, 'Brazil', 82);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int8
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt8OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetInt16

Retrieves type Int16 values from a dictionary.

**Syntax**

```sql
dictGetInt16('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int16` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int16
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Sweden', 32767);
INSERT INTO countries VALUES (2, 'Burundi', 28372);
INSERT INTO countries VALUES (3, 'Mozambique', 18372);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int16
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt16('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
28372
```

## dictGetInt16OrDefault

Retrieves type Int16 values from a dictionary. If the Int16 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetInt16OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int16` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int16
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Sweden', 32767);
INSERT INTO countries VALUES (2, 'Burundi', 28372);
INSERT INTO countries VALUES (3, 'Mozambique', 18372);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int16
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt16OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetInt32

Retrieves type Int32 values from a dictionary.

**Syntax**

```sql
dictGetInt32('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int32` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int32 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'United States', 2147483647);
INSERT INTO countries VALUES (2, 'Maldives', 1938273982);
INSERT INTO countries VALUES (3, 'India', 928372817);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt32('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
1938273982
```

## dictGetInt32OrDefault

Retrieves type Int32 values from a dictionary. If the Int32 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetInt32OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int32` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int32 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'United States', 2147483647);
INSERT INTO countries VALUES (2, 'Maldives', 1938273982);
INSERT INTO countries VALUES (3, 'India', 928372817);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt32OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetInt64

Retrieves type Int64 values from a dictionary.

**Syntax**

```sql
dictGetInt64('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int64` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int64 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Palau', 9223372036854775801);
INSERT INTO countries VALUES (2, 'Jamaica', 384982938291);
INSERT INTO countries VALUES (3, 'Tunisia', 910382938472827168);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt64('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
384982938291
```

## dictGetInt64OrDefault

Retrieves type Int64 values from a dictionary. If the Int64 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetInt64OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Int64` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation Int64 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Palau', 9223372036854775801);
INSERT INTO countries VALUES (2, 'Jamaica', 384982938291);
INSERT INTO countries VALUES (3, 'Tunisia', 910382938472827168);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt64OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetUInt8

Retrieves type UInt8 values from a dictionary.

**Syntax**

```sql
dictGetUInt8('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt8` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt8
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Mali', 32);
INSERT INTO countries VALUES (2, 'Czechia', 28);
INSERT INTO countries VALUES (3, 'Brazil', 82);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt8
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt8('country_codes', 'code_designation', '3') AS code_designation_output;
```

Result:

```response
28
```

## dictGetUInt8OrDefault

Retrieves type UInt8 values from a dictionary. If the UInt8 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetUInt8OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt8` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt8
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Mali', 32);
INSERT INTO countries VALUES (2, 'Czechia', 28);
INSERT INTO countries VALUES (3, 'Brazil', 82);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt8
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt8OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetUInt16

Retrieves type UInt16 values from a dictionary.

**Syntax**

```sql
dictGetUInt16('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt16` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt16
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Sweden', 32767);
INSERT INTO countries VALUES (2, 'Burundi', 28372);
INSERT INTO countries VALUES (3, 'Mozambique', 18372);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt16
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt16('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
28372
```

## dictGetUInt16OrDefault

Retrieves type UInt16 values from a dictionary. If the UInt16 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetUInt16OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt16` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt16
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Sweden', 32767);
INSERT INTO countries VALUES (2, 'Burundi', 28372);
INSERT INTO countries VALUES (3, 'Mozambique', 18372);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt16
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt16OrDefault('country_codes', 'code_designation', '999', 'Default value string.') AS code_designation_output;
```

Result:

```response
'Default value string.'
```

## dictGetUInt32

Retrieves type UInt32 values from a dictionary.

**Syntax**

```sql
dictGetUInt32('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt32` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt32 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'United States', 2147483647);
INSERT INTO countries VALUES (2, 'Maldives', 1938273982);
INSERT INTO countries VALUES (3, 'India', 928372817);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt32('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
1938273982
```

## dictGetUInt32OrDefault

Retrieves type UInt32 values from a dictionary. If the UInt32 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetUInt32OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt32` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt32 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'United States', 2147483647);
INSERT INTO countries VALUES (2, 'Maldives', 1938273982);
INSERT INTO countries VALUES (3, 'India', 928372817);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation UInt32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUInt32OrDefault('country_codes', 'code_designation', '999', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetUInt64

Retrieves type UInt64 values from a dictionary.

**Syntax**

```sql
dictGetUInt64('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt64` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt64 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Palau', 9223372036854775801);
INSERT INTO countries VALUES (2, 'Jamaica', 384982938291);
INSERT INTO countries VALUES (3, 'Tunisia', 910382938472827168);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt64('country_codes', 'code_designation', '2') AS code_designation_output;
```

Result:

```response
384982938291
```

## dictGetUInt64OrDefault

Retrieves type UInt64 values from a dictionary. If the UInt64 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetUInt64OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UInt64` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    code_designation UInt64 
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Palau', 9223372036854775801);
INSERT INTO countries VALUES (2, 'Jamaica', 384982938291);
INSERT INTO countries VALUES (3, 'Tunisia', 910382938472827168);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    code_designation Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetInt64OrDefault('country_codes', 'code_designation', '2', 'Default return string.') AS code_designation_output;
```

Result:

```response
'Default return string.'
```

## dictGetFloat32

Retrieves type Float32 values from a dictionary.

**Syntax**

```sql
dictGetFloat32('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Float32` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    float_designation Float32
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Andora', 1.329382);
INSERT INTO countries VALUES (2, 'Italy', 2.39137);
INSERT INTO countries VALUES (3, 'El Salvador', 4.03928);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    float_designation Float32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetFloat32('country_codes', 'float_designation', '2') AS dict_output;
```

Result:

```response
2.39137
```

## dictGetFloat32OrDefault

Retrieves type Float32 values from a dictionary. If the Float32 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetFloat32OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Float32` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    float_designation Float32
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Andora', 1.329382);
INSERT INTO countries VALUES (2, 'Italy', 2.39137);
INSERT INTO countries VALUES (3, 'El Salvador', 4.03928);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    float_designation Float32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetFloat32OrDefault('country_codes', 'float_designation', '999', 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetFloat64

Retrieves type Float64 values from a dictionary.

**Syntax**

```sql
dictGetFloat64('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Float64` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    float_designation Float64
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Madagascar', 1084781.1984689625);
INSERT INTO countries VALUES (2, 'Vanuatu', 109862.3);
INSERT INTO countries VALUES (3, 'Peru', 19474.19);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    float_designation Float32
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetFloat32('country_codes', 'float_designation', '1') AS dict_output;
```

Result:

```response
1084781.2
```

## dictGetFloat64OrDefault

Retrieves type Float64 values from a dictionary. If the Float64 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetFloat64OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Float64` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    float_designation Float64
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Madagascar', 1084781.1984689625);
INSERT INTO countries VALUES (2, 'Vanuatu', 109862.3);
INSERT INTO countries VALUES (3, 'Peru', 19474.19);

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    float_designation Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetFloat64OrDefault('country_codes', 'float_designation', '999', 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetDate

Retrieves type Date values from a dictionary.

**Syntax**

```sql
dictGetDate('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Date` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    independence_day Date
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Belarus', '1991-06-03');

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    independence_day Date
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetDate('country_codes', 'independence_day', '1') AS dict_output;
```

Result:

```response
1991-06-03
```

## dictGetDateOrDefault

Retrieves type Date values from a dictionary. If the Date attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetDateOrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `Date` type. If the attribute cannot be found, the `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    independence_day Date
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Belarus', '1991-06-03');

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    independence_day Date
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetDate('country_codes', 'independence_day', '100', 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetDateTime

Retrieves type DateTime values from a dictionary.

**Syntax**

```sql
dictGetDateTime('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `DateTime` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    independence_day DateTime
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Belarus', '1970-10-10 00:00:01');

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    independence_day DateTime
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetDateTime('country_codes', 'independence_day', '1') AS dict_output;
```

Result:

```response
1970-10-10 00:00:01
```

## dictGetDateTimeOrDefault

Retrieves type DateTime values from a dictionary. If the DateTime attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetDateTimeOrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `DateTime` type. If the attribute cannot be found, the `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt64,
    country String,
    independence_day DateTime
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, 'Belarus', '1970-10-10 00:00:01');

CREATE DICTIONARY country_codes
(
    id UInt64,
    country String,
    independence_day DateTime
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetDateTimeOrDefault('country_codes', 'independence_day', '999', 'Default return string.") AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetUUID

Retrieves type UUID values from a dictionary.

**Syntax**

```sql
dictGetUUID('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UUID` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    special_id UUID,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), ' Seychelles');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    special_id UUID,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUUID('countries_dict', 'special_id', 1) AS dict_output;
```

Result:

```response
0ee3c544-f178-4e93-9112-9fec9a58957b
```

## dictGetUUIDOrDefault

Retrieves type UUID values from a dictionary. If the UUID attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetUUIDOrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `UUID` type. If the attribute cannot be found, the `default_value_expr` is returned.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    special_id UUID,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), ' Seychelles');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    special_id UUID,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetUUIDOrDefault('countries_dict', 'special_id', 999, 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetString

Retrieves type String values from a dictionary.

**Syntax**

```sql
dictGetString('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `String` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), 'Philippines');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetString('countries_dict', 'country', 1) AS dict_output;
```

Result:

```response
Philippines
```

## dictGetStringOrDefault

Retrieves type String values from a dictionary. If the String attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetStringOrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `String` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), 'Philippines');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetStringOrDefault('countries_dict', 'country', 999, 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetIPv4

Retrieves type IPv4 values from a dictionary.

**Syntax**

```sql
dictGetIPv4('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `IPv4` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    ip_address IPv4,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), '103.202.232.0', 'Spain');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    ip_address IPv4,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetIPv4('countries_dict', 'ip_address', 1) AS dict_output;
```

Result:

```response
103.202.232.0
```

## dictGetIPv4OrDefault

Retrieves type IPv4 values from a dictionary. If the IPv4 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetIPv4OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `IPv4` type. If the attribute cannot be found, the value of `default_value_expr` is returned instead.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    ip_address IPv4,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), '103.202.232.0', 'Spain');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    ip_address IPv4,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetIPv4OrDefault('countries_dict', 'ip_address', 999, 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```

## dictGetIPv6

Retrieves type IPv6 values from a dictionary.

**Syntax**

```sql
dictGetIPv6('dictionary_name', 'attribute_name', key)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `IPv6` type.

**Implementation details**

This function will throw an exception if the key does not exist in the dictionary.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    ip_address IPv6,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), '0:0:0:0:0:ffff:67ca:e800', 'Spain');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    ip_address IPv6,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetIPv6('countries_dict', 'ip_address', 1) AS dict_output;
```

Result:

```response
::ffff:103.202.232.0
```

## dictGetIPv6OrDefault

Retrieves type IPv6 values from a dictionary. If the IPv6 attribute cannot be found, returns the specified default value instead.

**Syntax**

```sql
dictGetIPv6OrDefault('dictionary_name', 'attribute_name', key, default_value_expr)
```

**Arguments**

- `dictionary_name`: the name of the dictionary you want to retrieve data from. [String literal](../syntax#syntax-string-literal)
- `attribute_name`: the attribute whose value you want to get. [String literal](../syntax#syntax-string-literal), or [tuple](../data-types/tuple) of column names.
- `key`: the key associated with the attribute. [Expression](../syntax#syntax-expressions) returning dictionary key-type value.
- `default_value_expr` — Values returned if the dictionary does not contain a row with the `id_expr` key. [Expression](../../sql-reference/syntax.md#syntax-expressions) or [Tuple](../../sql-reference/data-types/tuple.md)([Expression](../../sql-reference/syntax.md#syntax-expressions)), returning the value (or values) in the data types configured for the `attr_names` attribute.

**Returned value**

The value of the dictionary attribute that corresponds to the given key. This value is parsed and returned as an `IPv6` type.

**Implementation details**

None.

**Examples**

Query:

```sql
CREATE TABLE countries
(
    id UInt8,
    unique_id UUID,
    ip_address IPv6,
    country String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO countries VALUES (1, generateUUIDv4(), '0:0:0:0:0:ffff:67ca:e800', 'Spain');

CREATE DICTIONARY countries_dict
(
    id UInt8, 
    unique_id UUID,
    ip_address IPv6,
    country String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'countries'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

SELECT dictGetIPv6OrDefault('countries_dict', 'ip_address', 999, 'Default return string.') AS dict_output;
```

Result:

```response
'Default return string.'
```
