---
description: 'Dictionary key and attribute configuration'
sidebar_label: 'Attributes'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Dictionary attributes'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

The `structure` clause describes the dictionary key and fields available for queries.

XML description:

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

Attributes are described in the elements:

- `<id>` — Key column
- `<attribute>` — Data column: there can be a multiple number of attributes.

DDL query:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Attributes are described in the query body:

- `PRIMARY KEY` — Key column
- `AttrName AttrType` — Data column. There can be a multiple number of attributes.

## Key {#key}

ClickHouse supports the following types of keys:

- Numeric key. `UInt64`. Defined in the `<id>` tag or using `PRIMARY KEY` keyword.
- Composite key. Set of values of different types. Defined in the tag `<key>` or `PRIMARY KEY` keyword.

An xml structure can contain either `<id>` or `<key>`. DDL-query must contain single `PRIMARY KEY`.

:::note
You must not describe key as an attribute.
:::

### Numeric Key {#numeric-key}

Type: `UInt64`.

Configuration example:

```xml
<id>
    <name>Id</name>
</id>
```

Configuration fields:

- `name` – The name of the column with keys.

For DDL-query:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

- `PRIMARY KEY` – The name of the column with keys.

### Composite Key {#composite-key}

The key can be a `tuple` from any types of fields. The [layout](./layouts/) in this case must be `complex_key_hashed` or `complex_key_cache`.

:::tip
A composite key can consist of a single element. This makes it possible to use a string as the key, for instance.
:::

The key structure is set in the element `<key>`. Key fields are specified in the same format as the dictionary [attributes](#dictionary-key-and-fields). Example:

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

or

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

For a query to the `dictGet*` function, a tuple is passed as the key. Example: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Attributes {#attributes}

Configuration example:

```xml
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

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Configuration fields:

| Tag                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Required |
|------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | Column name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Yes      |
| `type`                                               | ClickHouse data type: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br/>ClickHouse tries to cast value from dictionary to the specified data type. For example, for MySQL, the field might be `TEXT`, `VARCHAR`, or `BLOB` in the MySQL source table, but it can be uploaded as `String` in ClickHouse.<br/>[Nullable](../../../data-types/nullable.md) is currently supported for [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache#complex_key_cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache) dictionaries. In [IPTrie](./layouts/ip-trie) dictionaries `Nullable` types are not supported. | Yes      |
| `null_value`                                         | Default value for a non-existing element.<br/>In the example, it is an empty string. [NULL](../../../syntax.md#null) value can be used only for the `Nullable` types (see the previous line with types description).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Yes      |
| `expression`                                         | [Expression](../../../syntax.md#expressions) that ClickHouse executes on the value.<br/>The expression can be a column name in the remote SQL database. Thus, you can use it to create an alias for the remote column.<br/><br/>Default value: no expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | No       |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | If `true`, the attribute contains the value of a parent key for the current key. See [Hierarchical Dictionaries](./layouts/hierarchical).<br/><br/>Default value: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | No       |
| `injective`                                          | Flag that shows whether the `id -> attribute` image is [injective](https://en.wikipedia.org/wiki/Injective_function).<br/>If `true`, ClickHouse can automatically place after the `GROUP BY` clause the requests to dictionaries with injection. Usually it significantly reduces the amount of such requests.<br/><br/>Default value: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | No       |
| `is_object_id`                                       | Flag that shows whether the query is executed for a MongoDB document by `ObjectID`.<br/><br/>Default value: `false`.
