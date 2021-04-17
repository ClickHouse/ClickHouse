---
toc_priority: 44
toc_title: Dictionary Key and Fields
---

# Dictionary Key and Fields {#dictionary-key-and-fields}

The `<structure>` clause describes the dictionary key and fields available for queries.

XML description:

``` xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Attributes are described in the elements:

-   `<id>` — [Key column](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [Data column](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes). There can be a multiple number of attributes.

DDL query:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Attributes are described in the query body:

-   `PRIMARY KEY` — [Key column](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [Data column](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes). There can be a multiple number of attributes.

## Key {#ext_dict_structure-key}

ClickHouse supports the following types of keys:

-   Numeric key. `UInt64`. Defined in the `<id>` tag or using `PRIMARY KEY` keyword.
-   Composite key. Set of values of different types. Defined in the tag `<key>` or `PRIMARY KEY` keyword.

An xml structure can contain either `<id>` or `<key>`. DDL-query must contain single `PRIMARY KEY`.

!!! warning "Warning"
    You must not describe key as an attribute.

### Numeric Key {#ext_dict-numeric-key}

Type: `UInt64`.

Configuration example:

``` xml
<id>
    <name>Id</name>
</id>
```

Configuration fields:

-   `name` – The name of the column with keys.

For DDL-query:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### Composite Key {#composite-key}

The key can be a `tuple` from any types of fields. The [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) in this case must be `complex_key_hashed` or `complex_key_cache`.

!!! tip "Tip"
    A composite key can consist of a single element. This makes it possible to use a string as the key, for instance.

The key structure is set in the element `<key>`. Key fields are specified in the same format as the dictionary [attributes](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Example:

``` xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

or

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

For a query to the `dictGet*` function, a tuple is passed as the key. Example: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Attributes {#ext_dict_structure-attributes}

Configuration example:

``` xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

or

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Configuration fields:

| Tag                                                  | Description                                                                                                                                                                                                                                                                                                                                     | Required |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | Column name.                                                                                                                                                                                                                                                                                                                                    | Yes      |
| `type`                                               | ClickHouse data type.<br/>ClickHouse tries to cast value from dictionary to the specified data type. For example, for MySQL, the field might be `TEXT`, `VARCHAR`, or `BLOB` in the MySQL source table, but it can be uploaded as `String` in ClickHouse.<br/>[Nullable](../../../sql-reference/data-types/nullable.md) is not supported.       | Yes      |
| `null_value`                                         | Default value for a non-existing element.<br/>In the example, it is an empty string. You cannot use `NULL` in this field.                                                                                                                                                                                                                       | Yes      |
| `expression`                                         | [Expression](../../../sql-reference/syntax.md#syntax-expressions) that ClickHouse executes on the value.<br/>The expression can be a column name in the remote SQL database. Thus, you can use it to create an alias for the remote column.<br/><br/>Default value: no expression.                                                              | No       |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | If `true`, the attribute contains the value of a parent key for the current key. See [Hierarchical Dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).<br/><br/>Default value: `false`.                                                                                               | No       |
| `injective`                                          | Flag that shows whether the `id -> attribute` image is [injective](https://en.wikipedia.org/wiki/Injective_function).<br/>If `true`, ClickHouse can automatically place after the `GROUP BY` clause the requests to dictionaries with injection. Usually it significantly reduces the amount of such requests.<br/><br/>Default value: `false`. | No       |
| `is_object_id`                                       | Flag that shows whether the query is executed for a MongoDB document by `ObjectID`.<br/><br/>Default value: `false`.                                                                                                                                                                                                                            | No       |

## See Also {#see-also}

-   [Functions for working with external dictionaries](../../../sql-reference/functions/ext-dict-functions.md).

[Original article](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
