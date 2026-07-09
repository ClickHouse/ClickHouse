---
description: '辞書キーおよび属性の設定'
sidebar_label: '属性'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Dictionary の属性'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

`structure` 句では、クエリで利用できる辞書キーとフィールドを定義します。

XML の説明：

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

属性は次の要素で記述します:

* `<id>` — キーカラム
* `<attribute>` — データカラム: 属性は複数指定できます。

DDLクエリ:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

属性はクエリのボディ内で指定します：

* `PRIMARY KEY` — キーカラム
* `AttrName AttrType` — データカラム。属性は複数指定できます。

<div id="key">
  ## キー
</div>

ClickHouse は、次の種類のキーをサポートしています。

* 数値キー。`UInt64`。`<id>` タグ、または `PRIMARY KEY` キーワードで定義します。
* 複合キー。異なる型の値の集合です。`<key>` タグ、または `PRIMARY KEY` キーワードで定義します。

XML 構造には `<id>` または `<key>` のいずれか一方のみを含めることができます。DDL クエリには `PRIMARY KEY` を 1 つだけ含める必要があります。

:::note
キーを属性として記述してはいけません。
:::

<div id="numeric-key">
  ### 数値キー
</div>

型: `UInt64`。

設定例:

```xml
<id>
    <name>Id</name>
</id>
```

設定項目:

* `name` – キーを格納するカラム名。

DDLクエリの場合:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – キーを含むカラム名。

<div id="composite-key">
  ### 複合キー
</div>

キーには、任意の型のフィールドで構成された `tuple` を使用できます。この場合、[layout](./layouts/) は `complex_key_hashed` または `complex_key_cache` である必要があります。

:::tip
複合キーは 1 つの要素だけで構成することもできます。これにより、たとえば文字列をキーとして使用できます。
:::

キーの構造は `<key>` 要素で設定します。キーフィールドは、辞書の [属性](#attributes) と同じフォーマットで指定します。例:

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

または

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

`dictGet*` 関数へのクエリでは、キーとしてタプルを渡します。例: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`。

複合キーが 1 つの属性だけで構成されている場合、キー値は `tuple` で包まずにそのまま渡せます。たとえば、`dictGetString('dict_name', 'attr_name', 'key')` と `dictGetString('dict_name', 'attr_name', tuple('key'))` はどちらも有効です。

<div id="attributes">
  ## 属性
</div>

設定例:

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

または

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

設定項目:

| タグ                                                 | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 必須  |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| `name`                                             | カラム名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | はい  |
| `type`                                             | ClickHouseのデータ型: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md)。<br />ClickHouseは、Dictionaryの値を指定されたデータ型にキャストしようとします。たとえば MySQL では、MySQL のソーステーブル上のフィールドが `TEXT`、`VARCHAR`、`BLOB` であっても、ClickHouse では `String` として取り込めます。<br />[Nullable](../../../data-types/nullable.md) は現在、[Flat](./layouts/flat)、[Hashed](./layouts/hashed)、[ComplexKeyHashed](./layouts/hashed#complex_key_hashed)、[Direct](./layouts/direct)、[ComplexKeyDirect](./layouts/direct#complex_key_direct)、[RangeHashed](./layouts/range-hashed)、Polygon、[Cache](./layouts/cache)、[ComplexKeyCache](./layouts/cache)、[SSDCache](./layouts/ssd-cache)、[SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache) Dictionaryでサポートされています。[IPTrie](./layouts/ip-trie) Dictionaryでは `Nullable` 型はサポートされていません。 | はい  |
| `null_value`                                       | 存在しない要素のデフォルト値。<br />この例では空文字列です。[NULL](../../../syntax.md#null) 値を使用できるのは `Nullable` 型のみです (型の説明がある前の行を参照してください) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | はい  |
| `expression`                                       | ClickHouseが値に対して実行する[Expression](../../../syntax.md#expressions)。<br />この式には、リモートSQLデータベース内のカラム名を指定できます。そのため、リモートカラムのエイリアスを作成するために使用できます。<br /><br />デフォルト値: 式なし。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | いいえ |
| <a name="hierarchical-dict-attr" /> `hierarchical` | `true` の場合、この属性には現在のキーに対応する親キーの値が含まれます。[Hierarchical Dictionaries](./layouts/hierarchical) を参照してください。<br /><br />デフォルト値: `false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | いいえ |
| `injective`                                        | `id -> attribute` の写像が[injective](https://en.wikipedia.org/wiki/Injective_function) であるかどうかを示すフラグ。<br />`true` の場合、ClickHouseは `GROUP BY` clause の後に Dictionary への参照を自動的に配置できます。通常、これによりそのような参照の数を大幅に減らせます。<br /><br />デフォルト値: `false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | いいえ |
| `is_object_id`                                     | クエリが `ObjectID` によって MongoDB ドキュメントに対して実行されるかどうかを示すフラグ。<br /><br />デフォルト値: `false`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |     |