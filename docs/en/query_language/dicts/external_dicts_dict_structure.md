<a name="dicts-external_dicts_dict_structure"></a>

# Dictionary Key and Fields

The `<structure>` clause describes the dictionary key and fields available for queries.

Overall structure:

```xml
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

Columns are described in the structure:

- `<id>` - [key column](external_dicts_dict_structure.md#dicts-external_dicts_dict_structure-key).
- `<attribute>` - [data column](external_dicts_dict_structure.md#dicts-external_dicts_dict_structure-attributes). There can be a large number of columns.

<a name="dicts-external_dicts_dict_structure-key"></a>

## Key

ClickHouse supports the following types of keys:

- Numeric key. UInt64. Defined in the tag `<id>` .
- Composite key. Set of values of different types. Defined in the tag `<key>` .

A structure can contain either `<id>` or `<key>` .

!!! Note:
The key doesn't need to be defined separately in attributes.

### Numeric Key

Format: `UInt64`.

Configuration example:

```xml
<id>
    <name>Id</name>
</id>
```

Configuration fields:

- name – The name of the column with keys.

### Composite Key

The key can be a `tuple` from any types of fields. The [layout](external_dicts_dict_layout.md#dicts-external_dicts_dict_layout) in this case must be `complex_key_hashed` or `complex_key_cache`.

!!! Tip:
A composite key can consist of a single element. This makes it possible to use a string as the key, for instance.

The key structure is set in the element `<key>`. Key fields are specified in the same format as the dictionary [attributes](external_dicts_dict_structure.md#dicts-external_dicts_dict_structure-attributes). Example:

```xml
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

For a query to the `dictGet*` function, a tuple is passed as the key. Example: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

<a name="dicts-external_dicts_dict_structure-attributes"></a>

## Attributes

Configuration example:

```xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>Type</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

Configuration fields:

- `name` – The column name.
- `type` – The column type. Sets the method for interpreting data in the source. For example, for MySQL, the field might be `TEXT`, `VARCHAR`, or `BLOB` in the source table, but it can be uploaded as `String`.
- `null_value` – The default value for a non-existing element. In the example, it is an empty string.
- `expression` – The attribute can be an expression. The tag is not required.
- `hierarchical` – Hierarchical support. Mirrored to the parent identifier. By default, `false`.
- `injective` – Whether the `id -> attribute` image is injective. If `true`, then you can optimize the ` GROUP BY` clause. By default, `false`.
- `is_object_id` – Whether the query is executed for a MongoDB document by `ObjectID`.

