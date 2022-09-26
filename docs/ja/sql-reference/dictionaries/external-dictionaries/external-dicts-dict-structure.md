---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u8F9E\u66F8\u30AD\u30FC\u3068\u30D5\u30A3\u30FC\u30EB\u30C9"
---

# 辞書キーとフィールド {#dictionary-key-and-fields}

その `<structure>` 句クエリで使用できる辞書キーとフィールドを説明します。

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

-   `<id>` — [キー列](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [データ列](external-dicts-dict-structure.md#ext_dict_structure-attributes). 複数の属性が存在する可能性があります。

DDLクエリ:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

属性は、クエリの本文に記述されています:

-   `PRIMARY KEY` — [キー列](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [データ列](external-dicts-dict-structure.md#ext_dict_structure-attributes). 複数の属性が存在する可能性があります。

## キー {#ext_dict_structure-key}

ClickHouseは次の種類のキーをサポートしています:

-   数値キー。 `UInt64`. で定義される。 `<id>` タグまたは使用 `PRIMARY KEY` キーワード。
-   複合キー。 異なる型の値のセット。 タグで定義 `<key>` または `PRIMARY KEY` キーワード。

Xmlの構造を含むことができま `<id>` または `<key>`. DDL-クエリには単一を含む必要があります `PRIMARY KEY`.

!!! warning "警告"
    キーを属性として記述することはできません。

### 数値キー {#ext_dict-numeric-key}

タイプ: `UInt64`.

設定例:

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

キーはaである場合もあります `tuple` フィールドの任意のタイプから。 その [レイアウト](external-dicts-dict-layout.md) この場合、 `complex_key_hashed` または `complex_key_cache`.

!!! tip "ヒント"
    複合キーは、単一の要素で構成できます。 これにより、たとえば文字列をキーとして使用することができます。

キー構造は要素に設定されます `<key>`. キーフィールドは、辞書と同じ形式で指定されます [属性](external-dicts-dict-structure.md). 例:

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

クエリに対して `dictGet*` 関数は、タプルがキーとして渡されます。 例: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## 属性 {#ext_dict_structure-attributes}

設定例:

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

| タグ                                                 | 説明                                                                                                                                                                                                                                                                                                                                     | 必須     |
|------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | 列名。                                                                                                                                                                                                                                                                                                                                   | はい。   |
| `type`                                               | ClickHouseデータ型。<br/>ClickHouseは、dictionaryから指定されたデータ型に値をキャストしようとします。 例えば、MySQL、フィールドが `TEXT`, `VARCHAR`,または `BLOB` MySQLソーステーブルでは、次のようにアップロードできます `String` クリックハウスで<br/>[Null可能](../../../sql-reference/data-types/nullable.md) サポートされていない。 | はい。   |
| `null_value`                                         | 既存の要素以外の既定値。<br/>この例では、空の文字列です。 使用できません `NULL` この分野で。                                                                                                                                                                                                                                             | はい。   |
| `expression`                                         | [式](../../syntax.md#syntax-expressions) そのClickHouseは値に対して実行されます。<br/>式には、リモートSQLデータベースの列名を指定できます。 したがって、リモート列の別名を作成するために使用できます。<br/><br/>デフォルト値:式なし。                                                                                                    | いいえ。 |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | もし `true` この属性には、現在のキーの親キーの値が含まれます。 見る [階層辞書](external-dicts-dict-hierarchical.md).<br/><br/>デフォルト値: `false`.                                                                                                                                                                                     | いいえ。 |
| `injective`                                          | このフラグは `id -> attribute` 画像は [injective](https://en.wikipedia.org/wiki/Injective_function).<br/>もし `true`、ClickHouseはの後に自動的に置くことができます `GROUP BY` 句インジェクションを使用した辞書への要求。 通常、そのような要求の量を大幅に削減します。<br/><br/>デフォルト値: `false`.                                    | いいえ。 |
| `is_object_id`                                       | クエリがMongoDBドキュメントに対して実行されるかどうかを示すフラグ `ObjectID`.<br/><br/>デフォルト値: `false`.                                                                                                                                                                                                                            | いいえ。 |

## も参照。 {#see-also}

-   [外部辞書を操作するための関数](../../../sql-reference/functions/ext-dict-functions.md).

[元の記事](https://clickhouse.com/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
