---
description: '字典键与属性配置'
sidebar_label: '属性'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: '字典属性'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

`structure` 子句用于描述字典键以及可供查询的字段。

XML 描述：

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

属性通过以下元素进行描述：

* `<id>` — 键列
* `<attribute>` — 数据列：可以有多个属性。

DDL 查询：

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

属性在查询正文中说明如下：

* `PRIMARY KEY` — 键列
* `AttrName AttrType` — 数据列。属性可以有多个。

<div id="key">
  ## 键
</div>

ClickHouse 支持以下类型的键：

* 数值键。`UInt64`。在 `<id>` 标签中定义，或使用 `PRIMARY KEY` 关键字定义。
* 复合键。由不同类型的值组成。在 `<key>` 标签中定义，或使用 `PRIMARY KEY` 关键字定义。

XML 结构中只能包含 `<id>` 或 `<key>` 其中之一。DDL 查询语句中只能有一个 `PRIMARY KEY`。

:::note
不要将键描述为属性。
:::

<div id="numeric-key">
  ### 数值型键
</div>

类型：`UInt64`。

配置示例：

```xml
<id>
    <name>Id</name>
</id>
```

配置字段：

* `name` – 包含键的列名称。

对于 DDL 查询：

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – 键所在列的名称。

<div id="composite-key">
  ### 复合键
</div>

键可以是由任意类型字段组成的 `tuple`。在这种情况下，[layout](./layouts/) 必须为 `complex_key_hashed` 或 `complex_key_cache`。

:::tip
复合键也可以只包含一个元素。例如，这样就可以将字符串用作键。
:::

键的结构在 `<key>` 元素中定义。键字段的指定格式与字典[属性](#attributes)相同。示例：

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

或

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

对于对 `dictGet*` 函数的查询，键以元组形式传递。示例：`dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`。

当复合键仅包含单个属性时，可以直接传递键值，无需用 `tuple` 包装。例如，`dictGetString('dict_name', 'attr_name', 'key')` 和 `dictGetString('dict_name', 'attr_name', tuple('key'))` 都是有效的。

<div id="attributes">
  ## 属性
</div>

配置示例：

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

或

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

配置字段：

| 标签                                                 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | 必填 |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -- |
| `name`                                             | 列名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | 是  |
| `type`                                             | ClickHouse 数据类型：[UInt8](../../../data-types/int-uint.md)、[UInt16](../../../data-types/int-uint.md)、[UInt32](../../../data-types/int-uint.md)、[UInt64](../../../data-types/int-uint.md)、[Int8](../../../data-types/int-uint.md)、[Int16](../../../data-types/int-uint.md)、[Int32](../../../data-types/int-uint.md)、[Int64](../../../data-types/int-uint.md)、[Float32](../../../data-types/float.md)、[Float64](../../../data-types/float.md)、[UUID](../../../data-types/uuid.md)、[Decimal32](../../../data-types/decimal.md)、[Decimal64](../../../data-types/decimal.md)、[Decimal128](../../../data-types/decimal.md)、[Decimal256](../../../data-types/decimal.md)、[Date](../../../data-types/date.md)、[Date32](../../../data-types/date32.md)、[DateTime](../../../data-types/datetime.md)、[DateTime64](../../../data-types/datetime64.md)、[String](../../../data-types/string.md)、[Array](../../../data-types/array.md)。<br />ClickHouse 会尝试将字典中的值转换为指定的数据类型。例如，对于 MySQL，字段在 MySQL 源表中可能是 `TEXT`、`VARCHAR` 或 `BLOB`，但在 ClickHouse 中可以作为 `String` 导入。<br />[Nullable](../../../data-types/nullable.md) 当前支持 [Flat](./layouts/flat)、[Hashed](./layouts/hashed)、[ComplexKeyHashed](./layouts/hashed#complex_key_hashed)、[Direct](./layouts/direct)、[ComplexKeyDirect](./layouts/direct#complex_key_direct)、[RangeHashed](./layouts/range-hashed)、Polygon、[Cache](./layouts/cache)、[ComplexKeyCache](./layouts/cache)、[SSDCache](./layouts/ssd-cache)、[SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache) 字典。在 [IPTrie](./layouts/ip-trie) 字典中，不支持 `Nullable` 类型。 | 是  |
| `null_value`                                       | 不存在元素的默认值。<br />在该示例中，它是空字符串。[NULL](../../../syntax.md#null) 值只能用于 `Nullable` 类型 (请参见上一行的类型说明) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | 是  |
| `expression`                                       | ClickHouse 在该值上执行的 [表达式](../../../syntax.md#expressions)。<br />该表达式可以是远程 SQL 数据库中的列名。因此，你可以用它为远程列创建别名。<br /><br />默认值：无表达式。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 否  |
| <a name="hierarchical-dict-attr" /> `hierarchical` | 如果为 `true`，则该属性包含当前键对应父键的值。请参见[层级字典](./layouts/hierarchical)。<br /><br />默认值：`false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 否  |
| `injective`                                        | 用于指示 `id -> attribute` 映射是否为[单射](https://en.wikipedia.org/wiki/Injective_function)的标志。<br />如果为 `true`，ClickHouse 可以自动将对具有单射属性的字典的请求放在 `GROUP BY` 子句之后。这通常会显著减少此类请求的数量。<br /><br />默认值：`false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 否  |
| `is_object_id`                                     | 用于指示是否通过 `ObjectID` 对 MongoDB 文档执行查询的标志。<br /><br />默认值：`false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |    |