---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 44
toc_title: "\u8F9E\u66F8\u306E\u30AD\u30FC\u3068\u30D5\u30A3\u30FC\u30EB\u30C9"
---

# 辞書のキーとフィールド {#dictionary-key-and-fields}

その `<structure>` 条項の辞書のキーや分野での利用ます。

XMLの説明:

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

属性は要素に記述されています:

-   `<id>` — [キー列](external_dicts_dict_structure.md#ext_dict_structure-key).
-   `<attribute>` — [データ列](external_dicts_dict_structure.md#ext_dict_structure-attributes). 複数の属性を指定できます。

DDLクエリ:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

属性はクエリ本文に記述されます:

-   `PRIMARY KEY` — [キー列](external_dicts_dict_structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [データ列](external_dicts_dict_structure.md#ext_dict_structure-attributes). 複数の属性を指定できます。

## キー {#ext_dict_structure-key}

ClickHouseは次の種類のキーをサポートしています:

-   数値キー。 `UInt64`. で定義される `<id>` タグまたは使用 `PRIMARY KEY` キーワード。
-   複合キー。 異なるタイプの値のセット。 タグ内で定義されている `<key>` または `PRIMARY KEY` キーワード。

Xmlの構造を含むことができま `<id>` または `<key>`. DDL-クエリにsingleを含める必要があります `PRIMARY KEY`.

!!! warning "警告"
    Keyを属性として記述することはできません。

### 数値キー {#ext_dict-numeric-key}

タイプ: `UInt64`.

構成例:

``` xml
<id>
    <name>Id</name>
</id>
```

設定フィールド:

-   `name` – The name of the column with keys.

DDLクエリの場合:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### 複合キー {#composite-key}

キーは次のようになります `tuple` フィールドの任意のタイプから。 その [レイアウト](external_dicts_dict_layout.md) この場合、 `complex_key_hashed` または `complex_key_cache`.

!!! tip "ヒント"
    複合キーは、単一の要素で構成できます。 これにより、たとえば文字列をキーとして使用することができます。

キー構造は要素で設定されます `<key>`. キーフィールドは、ディクショナリと同じ形式で指定します [属性](external_dicts_dict_structure.md). 例えば:

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

または

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

クエリの場合 `dictGet*` 関数は、タプルがキーとして渡されます。 例えば: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## 属性 {#ext_dict_structure-attributes}

構成例:

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

または

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

設定フィールド:

| タグ                                                 | 説明                                                                                                                                                                                                                                                                                                                                                                    | 必須     |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | 列名。                                                                                                                                                                                                                                                                                                                                                                  | はい。   |
| `type`                                               | ClickHouseデータタイプ。<br/>ClickHouseは、dictionaryから指定されたデータ型に値をキャストしようとします。 例えば、MySQLの場合、フィールドは次のようになります `TEXT`, `VARCHAR`、または `BLOB` MySQLソーステーブルでは、次のようにアップロードできます `String` クリックハウスで。<br/>[Nullable](../../../sql_reference/data_types/nullable.md) サポートされていない。 | はい。   |
| `null_value`                                         | 既存の要素以外の要素のデフォルト値。<br/>この例では、空の文字列です。 使用できません `NULL` この分野で。                                                                                                                                                                                                                                                                | はい。   |
| `expression`                                         | [式](../../syntax.md#syntax-expressions) そのClickHouseはその値を実行します。<br/>この式には、リモートsqlデータベースの列名を指定できます。 したがって、これを使用して、リモート列の別名を作成できます。<br/><br/>デフォルト値:式なし。                                                                                                                                 | いいえ。 |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | もし `true`、属性は、現在のキーの親キーの値が含まれています。 見る [階層辞書](external_dicts_dict_hierarchical.md).<br/><br/>デフォルト値: `false`.                                                                                                                                                                                                                     | いいえ。 |
| `injective`                                          | このフラグは、 `id -> attribute` 画像は [射影](https://en.wikipedia.org/wiki/Injective_function).<br/>もし `true`、ClickHouseはの後に自動的に置くことができます `GROUP BY` 句注入を伴う辞書への要求。 通常、そのような要求の量が大幅に削減されます。<br/><br/>デフォルト値: `false`.                                                                                    | いいえ。 |
| `is_object_id`                                       | MongoDBドキュメントに対してクエリが実行されるかどうかを示すフラグ `ObjectID`.<br/><br/>デフォルト値: `false`.                                                                                                                                                                                                                                                           | いいえ。 |

## また見なさい {#see-also}

-   [外部辞書を操作するための関数](../../../sql_reference/functions/ext_dict_functions.md).

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
