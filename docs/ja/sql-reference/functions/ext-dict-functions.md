---
slug: /ja/sql-reference/functions/ext-dict-functions
sidebar_position: 50
sidebar_label: Dictionary
---

# ダイナミックDictionaryに対する操作の関数

:::note
[DDLクエリ](../../sql-reference/statements/create/dictionary.md)で作成されたDictionaryについては、`dict_name`パラメータを`<database>.<dict_name>`の形で完全に指定する必要があります。そうでない場合、現在のデータベースが使用されます。
:::

Dictionaryの接続および設定に関する情報は、[Dictionary](../../sql-reference/dictionaries/index.md)を参照してください。

## dictGet, dictGetOrDefault, dictGetOrNull

Dictionaryから値を取得します。

```sql
dictGet('dict_name', attr_names, id_expr)
dictGetOrDefault('dict_name', attr_names, id_expr, default_value_expr)
dictGetOrNull('dict_name', attr_name, id_expr)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `attr_names` — Dictionaryのカラム名。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)、またはカラム名のタプル。[タプル](../data-types/tuple.md)([文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal))。
- `id_expr` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で、Dictionary設定に応じたキータイプの値または[タプル](../data-types/tuple.md)型の値を返します。
- `default_value_expr` — Dictionaryに`id_expr`キーの行が含まれていない場合に返される値。[式](../../sql-reference/syntax.md#syntax-expressions)または[タプル](../data-types/tuple.md)([式](../../sql-reference/syntax.md#syntax-expressions))で、`attr_names`属性に設定された型で値を返します。

**返り値**

- ClickHouseが属性を[属性のデータ型](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes)で正常に解析すると、関数は`id_expr`に対応するDictionary属性の値を返します。

- Dictionaryに`id_expr`に対応するキーがない場合：

    - `dictGet`は、Dictionary設定で属性に指定された`<null_value>`要素の内容を返します。
    - `dictGetOrDefault`は、`default_value_expr`パラメータとして渡された値を返します。
    - `dictGetOrNull`は、キーがDictionaryで見つからなかった場合、`NULL`を返します。

ClickHouseは、属性の値を解析できない場合や、値が属性のデータ型と一致しない場合に例外を投げます。

**シンプルキーDictionaryの例**

以下の内容のテキストファイル`ext-dict-test.csv`を作成します：

```text
1,1
2,2
```

最初のカラムは`id`、2番目のカラムは`c1`です。

Dictionaryを設定します：

```xml
<clickhouse>
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
</clickhouse>
```

クエリを実行します：

```sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

```text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**複雑なキーDictionaryの例**

以下の内容のテキストファイル`ext-dict-mult.csv`を作成します：

```text
1,1,'1'
2,2,'2'
3,3,'3'
```

最初のカラムは`id`、2番目は`c1`、3番目は`c2`です。

Dictionaryを設定します：

```xml
<clickhouse>
    <dictionary>
        <name>ext-dict-mult</name>
        <source>
            <file>
                <path>/path-to/ext-dict-mult.csv</path>
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
            <attribute>
                <name>c2</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</clickhouse>
```

クエリを実行します：

```sql
SELECT
    dictGet('ext-dict-mult', ('c1','c2'), number + 1) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

```text
┌─val─────┬─type──────────────────┐
│ (1,'1') │ Tuple(UInt8, String)  │
│ (2,'2') │ Tuple(UInt8, String)  │
│ (3,'3') │ Tuple(UInt8, String)  │
└─────────┴───────────────────────┘
```

**範囲キーDictionaryの例**

入力テーブル：

```sql
CREATE TABLE range_key_dictionary_source_table
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
ENGINE = TinyLog();

INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
```

Dictionaryを作成します：

```sql
CREATE DICTIONARY range_key_dictionary
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);
```

クエリを実行します：

```sql
SELECT
    (number, toDate('2019-05-20')),
    dictHas('range_key_dictionary', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value_nullable', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', ('value', 'value_nullable'), number, toDate('2019-05-20'))
FROM system.numbers LIMIT 5 FORMAT TabSeparated;
```
結果：

```text
(0,'2019-05-20')        0       \N      \N      (NULL,NULL)
(1,'2019-05-20')        1       First   First   ('First','First')
(2,'2019-05-20')        1       Second  \N      ('Second',NULL)
(3,'2019-05-20')        1       Third   Third   ('Third','Third')
(4,'2019-05-20')        0       \N      \N      (NULL,NULL)
```

**関連項目**

- [Dictionaries](../../sql-reference/dictionaries/index.md)

## dictHas

キーがDictionaryに存在するかどうかをチェックします。

```sql
dictHas('dict_name', id_expr)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `id_expr` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で、Dictionary設定に応じたキータイプの値または[タプル](../data-types/tuple.md)型の値を返します。

**返り値**

- キーが存在しない場合は0。[UInt8](../data-types/int-uint.md)。
- キーが存在する場合は1。[UInt8](../data-types/int-uint.md)。

## dictGetHierarchy

[階層型 Dictionary](../../sql-reference/dictionaries/index.md#hierarchical-dictionaries)内のキーのすべての親を含む配列を作成します。

**構文**

```sql
dictGetHierarchy('dict_name', key)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `key` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で[UInt64](../data-types/int-uint.md)型の値を返します。

**返り値**

- キーの親。[Array(UInt64)](../data-types/array.md)。

## dictIsIn

Dictionary内の階層全体を通して、キーの先祖をチェックします。

```sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `child_id_expr` — チェックされるキー。[式](../../sql-reference/syntax.md#syntax-expressions)で[UInt64](../data-types/int-uint.md)型の値を返します。
- `ancestor_id_expr` — `child_id_expr`キーの仮定された先祖。[式](../../sql-reference/syntax.md#syntax-expressions)で[UInt64](../data-types/int-uint.md)型の値を返します。

**返り値**

- `child_id_expr`が`ancestor_id_expr`の子でない場合は0。[UInt8](../data-types/int-uint.md)。
- `child_id_expr`が`ancestor_id_expr`の子または`ancestor_id_expr`自体である場合は1。[UInt8](../data-types/int-uint.md)。

## dictGetChildren

第一レベルの子をインデックスの配列として返します。[dictGetHierarchy](#dictgethierarchy) の逆の変換です。

**構文**

```sql
dictGetChildren(dict_name, key)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `key` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で[UInt64](../data-types/int-uint.md)型の値を返します。

**返り値**

- キーの第一レベルの子。[Array](../data-types/array.md)([UInt64](../data-types/int-uint.md))。

**例**

階層型のDictionaryを考慮します：

```text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```

第一レベルの子：

```sql
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

```text
┌─dictGetChildren('hierarchy_flat_dictionary', number)─┐
│ [1]                                                  │
│ [2,3]                                                │
│ [4]                                                  │
│ []                                                   │
└──────────────────────────────────────────────────────┘
```

## dictGetDescendant

全ての子孫を[dictGetChildren](#dictgetchildren)関数が`level`回の再帰で適用されたかのように返します。

**構文**

```sql
dictGetDescendants(dict_name, key, level)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `key` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で[UInt64](../data-types/int-uint.md)型の値を返します。
- `level` — 階層レベル。`level = 0`の場合、最後までの全ての子孫を返します。[UInt8](../data-types/int-uint.md)。

**返り値**

- キーの子孫。[Array](../data-types/array.md)([UInt64](../data-types/int-uint.md))。

**例**

階層型のDictionaryを考慮します：

```text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```
全ての子孫：

```sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

```text
┌─dictGetDescendants('hierarchy_flat_dictionary', number)─┐
│ [1,2,3,4]                                               │
│ [2,3,4]                                                 │
│ [4]                                                     │
│ []                                                      │
└─────────────────────────────────────────────────────────┘
```

第一レベルの子孫：

```sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 4;
```

```text
┌─dictGetDescendants('hierarchy_flat_dictionary', number, 1)─┐
│ [1]                                                        │
│ [2,3]                                                      │
│ [4]                                                        │
│ []                                                         │
└────────────────────────────────────────────────────────────┘
```


## dictGetAll

[正規表現ツリーDictionary](../../sql-reference/dictionaries/index.md#regexp-tree-dictionary)で各キーに一致したすべてのノードの属性値を取得します。

`Array(T)`型の値を返す点を除き、[`dictGet`](#dictget-dictgetordefault-dictgetornull)と同様に動作します。

**構文**

```sql
dictGetAll('dict_name', attr_names, id_expr[, limit])
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `attr_names` — Dictionaryのカラム名。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)または、カラム名のタプル。[タプル](../data-types/tuple.md)([文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal))。
- `id_expr` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で、Dictionary設定に応じたキータイプの値の配列または[タプル](../data-types/tuple.md)型の値を返します。
- `limit` - 返される各値配列の最大長。切り詰める場合、子ノードが親ノードより優先され、それ以外の場合は正規表現ツリーDictionaryで定義されたリストの順序が尊重されます。指定しない場合、配列の長さに制限はありません。

**返り値**

- ClickHouseが属性を正常に解析し、Dictionaryで定義された属性のデータ型として、各`attr_names`で指定された属性に対する`id_expr`に対応するDictionary属性値の配列を返します。

- Dictionaryに`id_expr`に対応するキーがない場合、空の配列が返されます。

ClickHouseは、属性の値を解析できない場合や、値が属性のデータ型と一致しない場合に例外をスローします。

**例**

以下のような正規表現ツリーDictionaryを考慮します：

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'foo'
  tag: 'foo_attr'
- regexp: 'bar'
  tag: 'bar_attr'
- regexp: 'baz'
  tag: 'baz_attr'
```

すべての一致する値を取得：

```sql
SELECT dictGetAll('regexp_dict', 'tag', 'foobarbaz');
```

```text
┌─dictGetAll('regexp_dict', 'tag', 'foobarbaz')─┐
│ ['foo_attr','bar_attr','baz_attr']            │
└───────────────────────────────────────────────┘
```

最大2つの一致する値を取得：

```sql
SELECT dictGetAll('regexp_dict', 'tag', 'foobarbaz', 2);
```

```text
┌─dictGetAll('regexp_dict', 'tag', 'foobarbaz', 2)─┐
│ ['foo_attr','bar_attr']                          │
└──────────────────────────────────────────────────┘
```

## その他の関数

ClickHouseは、Dictionaryの設定に関係なく、Dictionaryの属性値を特定のデータ型に変換する専門の関数をサポートしています。

関数:

- `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
- `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
- `dictGetFloat32`, `dictGetFloat64`
- `dictGetDate`
- `dictGetDateTime`
- `dictGetUUID`
- `dictGetString`
- `dictGetIPv4`, `dictGetIPv6`

これらの関数はすべて`OrDefault`の修飾が可能です。例えば、`dictGetDateOrDefault`のように。

構文：

```sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**引数**

- `dict_name` — Dictionaryの名前。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `attr_name` — Dictionaryのカラム名。[文字列リテラル](../../sql-reference/syntax.md#syntax-string-literal)。
- `id_expr` — キーの値。[式](../../sql-reference/syntax.md#syntax-expressions)で、[UInt64](../data-types/int-uint.md)または[タプル](../data-types/tuple.md)型の値を返します。
- `default_value_expr` — Dictionaryに`id_expr`キーの行が含まれていない場合に返される値。[式](../../sql-reference/syntax.md#syntax-expressions)で、`attr_name`に設定されたデータ型で値を返します。

**返り値**

- ClickHouseが属性を[属性のデータ型](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes)で正常に解析すると、関数は`id_expr`に対応するDictionary属性の値を返します。

- Dictionaryにリクエストされた`id_expr`がない場合：

    - `dictGet[Type]`は、Dictionary設定で属性に指定された`<null_value>`要素の内容を返します。
    - `dictGet[Type]OrDefault`は、`default_value_expr`パラメータとして渡された値を返します。

ClickHouseは、属性の値を解析できない場合や、値が属性のデータ型と一致しない場合に例外をスローします。
