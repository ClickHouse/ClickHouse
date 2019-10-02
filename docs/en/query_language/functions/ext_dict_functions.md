# Functions for Working with External Dictionaries {#ext_dict_functions}

For information on connecting and configuring external dictionaries, see [External dictionaries](../dicts/external_dicts.md).

## dictGet

Retrieves a value from an external dictionary.

```sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parameters**

- `dict_name` — Name of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `attr_name` — Name of the column of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md) or [Tuple](../../data_types/tuple.md)-type value depending on the dictionary configuration.
- `default_value_expr` — Value returned if the dictionary doesn't contain a row with the `id_expr` key. [Expression](../syntax.md#syntax-expressions) returning the value in the data type configured for the `attr_name` attribute.

**Returned value**

- If ClickHouse parses the attribute successfully in the [attribute's data type](../dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes), functions return the value of the dictionary attribute that corresponds to `id_expr`.
- If there is no the key, corresponding to `id_expr`, in the dictionary, then:

    - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
    - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse throws an exception if it cannot parse the value of the attribute or the value doesn't match the attribute data type.

**Example**

Create a text file `ext-dict-text.csv` containing the following:

```text
1,1
2,2
```

The first column is `id`, the second column is `c1`.

Configure the external dictionary:

```xml
<yandex>
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
</yandex>
```

Perform the query:

```sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```
```text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**See Also**

- [External Dictionaries](../dicts/external_dicts.md)


## dictHas

Checks whether a key is present in a dictionary.

```sql
dictHas('dict_name', id_expr)
```

**Parameters**

- `dict_name` — Name of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md)-type value.

**Returned value**

- 0, if there is no key.
- 1, if there is a key.

Type: `UInt8`.

## dictGetHierarchy

For the hierarchical dictionary, returns an array of dictionary keys starting from the passed `id_expr` and continuing along the chain of parent elements.

```sql
dictGetHierarchy('dict_name', id_expr)
```

**Parameters**

- `dict_name` — Name of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md)-type value.

**Returned value**

Hierarchy of dictionary keys.

Type: Array(UInt64).

## dictIsIn

Checks the ancestor of a key through the whole hierarchical chain in the dictionary.

```sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Parameters**

- `dict_name` — Name of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `child_id_expr` — Key to be checked. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md)-type value.
- `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` key. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md)-type value.

**Returned value**

- 0, if `child_id_expr` is not a child of `ancestor_id_expr`.
- 1, if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`.

Type: `UInt8`.

## Other functions {#ext_dict_functions-other}

ClickHouse supports specialized functions that convert dictionary attribute values to a specific data type regardless of the dictionary configuration.

Functions:

- `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
- `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
- `dictGetFloat32`, `dictGetFloat64`
- `dictGetDate`
- `dictGetDateTime`
- `dictGetUUID`
- `dictGetString`

All these functions have the `OrDefault` modification. For example, `dictGetDateOrDefault`.

Syntax:

```sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parameters**

- `dict_name` — Name of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `attr_name` — Name of the column of the dictionary. [String literal](../syntax.md#syntax-string-literal).
- `id_expr` — Key value. [Expression](../syntax.md#syntax-expressions) returning a [UInt64](../../data_types/int_uint.md)-type value.
- `default_value_expr` — Value which is returned if the dictionary doesn't contain a row with the `id_expr` key. [Expression](../syntax.md#syntax-expressions) returning a value in the data type configured for the `attr_name` attribute.

**Returned value**

- If ClickHouse parses the attribute successfully in the [attribute's data type](../dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes), functions return the value of the dictionary attribute that corresponds to `id_expr`.
- If there is no requested `id_expr` in the dictionary then:

    - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
    - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse throws an exception if it cannot parse the value of the attribute or the value doesn't match the attribute data type.

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
