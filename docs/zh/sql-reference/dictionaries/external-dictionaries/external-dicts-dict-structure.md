---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u5B57\u5178\u952E\u548C\u5B57\u6BB5"
---

# 字典键和字段 {#dictionary-key-and-fields}

该 `<structure>` 子句描述可用于查询的字典键和字段。

XML描述:

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

属性在元素中描述:

-   `<id>` — [键列](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [数据列](external-dicts-dict-structure.md#ext_dict_structure-attributes). 可以有多个属性。

DDL查询:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

查询正文中描述了属性:

-   `PRIMARY KEY` — [键列](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [数据列](external-dicts-dict-structure.md#ext_dict_structure-attributes). 可以有多个属性。

## 键 {#ext_dict_structure-key}

ClickHouse支持以下类型的键:

-   数字键。 `UInt64`. 在定义 `<id>` 标记或使用 `PRIMARY KEY` 关键字。
-   复合密钥。 组不同类型的值。 在标签中定义 `<key>` 或 `PRIMARY KEY` 关键字。

Xml结构可以包含 `<id>` 或 `<key>`. DDL-查询必须包含单个 `PRIMARY KEY`.

!!! warning "警告"
    不能将键描述为属性。

### 数字键 {#ext_dict-numeric-key}

类型: `UInt64`.

配置示例:

``` xml
<id>
    <name>Id</name>
</id>
```

配置字段:

-   `name` – The name of the column with keys.

对于DDL-查询:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### 复合密钥 {#composite-key}

关键可以是一个 `tuple` 从任何类型的字段。 该 [布局](external-dicts-dict-layout.md) 在这种情况下，必须是 `complex_key_hashed` 或 `complex_key_cache`.

!!! tip "提示"
    复合键可以由单个元素组成。 例如，这使得可以使用字符串作为键。

键结构在元素中设置 `<key>`. 键字段的格式与字典的格式相同 [属性](external-dicts-dict-structure.md). 示例:

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

或

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

对于查询 `dictGet*` 函数中，一个元组作为键传递。 示例: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## 属性 {#ext_dict_structure-attributes}

配置示例:

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

或

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

配置字段:

| 标签                                                 | 产品描述                                                                                                                                                                                                                                                      | 必填项 |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| `name`                                               | 列名称。                                                                                                                                                                                                                                                      | 是     |
| `type`                                               | ClickHouse数据类型。<br/>ClickHouse尝试将字典中的值转换为指定的数据类型。 例如，对于MySQL，该字段可能是 `TEXT`, `VARCHAR`，或 `BLOB` 在MySQL源表中，但它可以上传为 `String` 在克里克豪斯<br/>[可为空](../../../sql-reference/data-types/nullable.md) 不支持。 | 是     |
| `null_value`                                         | 非现有元素的默认值。<br/>在示例中，它是一个空字符串。 你不能使用 `NULL` 在这个领域。                                                                                                                                                                          | 是     |
| `expression`                                         | [表达式](../../syntax.md#syntax-expressions) ClickHouse对该值执行。<br/>表达式可以是远程SQL数据库中的列名。 因此，您可以使用它为远程列创建别名。<br/><br/>默认值：无表达式。                                                                                  | 非也。 |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | 如果 `true`，该属性包含当前键的父键值。 看 [分层字典](external-dicts-dict-hierarchical.md).<br/><br/>默认值: `false`.                                                                                                                                         | 非也。 |
| `injective`                                          | 标志，显示是否 `id -> attribute` 图像是 [注射](https://en.wikipedia.org/wiki/Injective_function).<br/>如果 `true`，ClickHouse可以自动放置后 `GROUP BY` 子句注入字典的请求。 通常它显着减少了这种请求的数量。<br/><br/>默认值: `false`.                        | 非也。 |
| `is_object_id`                                       | 显示是否通过以下方式对MongoDB文档执行查询的标志 `ObjectID`.<br/><br/>默认值: `false`.                                                                                                                                                                         | 非也。 |

## 另请参阅 {#see-also}

-   [使用外部字典的函数](../../../sql-reference/functions/ext-dict-functions.md).

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
