---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u5916\u90E8\u8F9E\u66F8\u306E\u64CD\u4F5C"
---

# 外部辞書を操作するための関数 {#ext_dict_functions}

情報の接続や設定の外部辞書参照 [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet {#dictget}

外部ディクショナリから値を取得します。

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**パラメータ**

-   `dict_name` — Name of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md) または [タプル](../../sql-reference/data-types/tuple.md)-辞書構成に応じて値を入力します。
-   `default_value_expr` — Value returned if the dictionary doesn't contain a row with the `id_expr` キー [式](../syntax.md#syntax-expressions) データ型の値を返します。 `attr_name` 属性。

**戻り値**

-   ClickHouseが属性を正常に解析した場合、 [属性のデータ型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes),関数は、に対応する辞書属性の値を返します `id_expr`.

-   キーがない場合、対応する `id_expr`、辞書では、:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouseは、属性の値を解析できない場合、または値が属性データ型と一致しない場合に例外をスローします。

**例**

テキストファイルの作成 `ext-dict-text.csv` 以下を含む:

``` text
1,1
2,2
```

最初の列は次のとおりです `id` 二つ目の列は `c1`.

外部辞書の構成:

``` xml
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

クエリの実行:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**も参照。**

-   [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)

## ディクタス {#dicthas}

キーが辞書に存在するかどうかを確認します。

``` sql
dictHas('dict_name', id_expr)
```

**パラメータ**

-   `dict_name` — Name of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ値。

**戻り値**

-   キーがない場合は0。
-   1、キーがある場合。

タイプ: `UInt8`.

## dictGetHierarchy {#dictgethierarchy}

キーのすべての親を含む配列を作成します。 [階層辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).

**構文**

``` sql
dictGetHierarchy('dict_name', key)
```

**パラメータ**

-   `dict_name` — Name of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `key` — Key value. [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ値。

**戻り値**

-   鍵の親。

タイプ: [配列(UInt64)](../../sql-reference/data-types/array.md).

## ジクチシン {#dictisin}

辞書の階層チェーン全体を通して、キーの祖先をチェックします。

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**パラメータ**

-   `dict_name` — Name of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `child_id_expr` — Key to be checked. [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ値。
-   `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` キー [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ値。

**戻り値**

-   0の場合 `child_id_expr` の子ではありません `ancestor_id_expr`.
-   1の場合 `child_id_expr` の子である `ancestor_id_expr` または `child_id_expr` は `ancestor_id_expr`.

タイプ: `UInt8`.

## その他の機能 {#ext_dict_functions-other}

ClickHouseは、辞書構成に関係なく、辞書属性の値を特定のデータ型に変換する特殊な関数をサポートしています。

関数:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

これらの関数はすべて、 `OrDefault` 修正 例えば, `dictGetDateOrDefault`.

構文:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**パラメータ**

-   `dict_name` — Name of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [文字列リテラル](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [式](../syntax.md#syntax-expressions) aを返す [UInt64](../../sql-reference/data-types/int-uint.md)-タイプ値。
-   `default_value_expr` — Value which is returned if the dictionary doesn't contain a row with the `id_expr` キー [式](../syntax.md#syntax-expressions) データ型に設定された値を返す `attr_name` 属性。

**戻り値**

-   ClickHouseが属性を正常に解析した場合、 [属性のデータ型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes),関数は、に対応する辞書属性の値を返します `id_expr`.

-   要求がない場合 `id_expr` 辞書では:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouseは、属性の値を解析できない場合、または値が属性データ型と一致しない場合に例外をスローします。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
