---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_title: "JSON\u3067\u306E\u4F5C\u696D"
---

# JSONを操作するための関数 {#functions-for-working-with-json}

Yandexので。Metrica、JSONで送信したユーザーとしてセッションパラメータ。 このJSONを操作するための特別な関数がいくつかあります。 （ほとんどの場合、Jsonはさらに前処理され、結果の値は処理された形式で別々の列に入れられます。）これらの関数はすべて、JSONが何であるかについての強い前提に基づいていますが、できるだけ少なくして仕事を終わらせようとします。

以下の仮定が行われます:

1.  フィールド名(関数の引数)は定数でなければなりません。
2.  フィールド名は何らかの形でjsonでエンコードされます。 例えば: `visitParamHas('{"abc":"def"}', 'abc') = 1` でも `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  フィールドは、任意の入れ子レベルで無差別に検索されます。 複数の一致するフィールドがある場合は、最初の出現が使用されます。
4.  JSONには文字列リテラルの外側に空白文字はありません。

## visitParamHas(パラムス、名前) {#visitparamhasparams-name}

があるかどうかをチェックします。 ‘name’ 名前

## ビジットパラメクストラクチュイント(params,name) {#visitparamextractuintparams-name}

名前のフィールドの値からUInt64を解析します ‘name’. これが文字列フィールドの場合、文字列の先頭から数値を解析しようとします。 フィールドが存在しない場合、または存在するが数値が含まれていない場合は、0を返します。

## ビジットパラメクストラクチント(params,name) {#visitparamextractintparams-name}

Int64と同じです。

## パラメーター) {#visitparamextractfloatparams-name}

Float64と同じです。

## ビジットパラメクストラクトブール(params,name) {#visitparamextractboolparams-name}

真/偽の値を解析します。 結果はUInt8です。

## ビジットパラメクストラクトロー(params,name) {#visitparamextractrawparams-name}

区切り文字を含むフィールドの値を返します。

例:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## パラメーターを指定します) {#visitparamextractstringparams-name}

二重引用符で文字列を解析します。 値はエスケープされていません。 エスケープ解除に失敗した場合は、空の文字列を返します。

例:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

現在、この形式のコードポイントはサポートされていません `\uXXXX\uYYYY` それは基本的な多言語面からではありません（UTF-8の代わりにCESU-8に変換されます）。

以下の機能は、次のとおりです [simdjson](https://github.com/lemire/simdjson) より複雑なJSON解析要件用に設計されています。 上記の仮定2はまだ適用されます。

## isValidJSON(json) {#isvalidjsonjson}

渡された文字列が有効なjsonであることを確認します。

例:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

値がJSONドキュメントに存在する場合, `1` 返されます。

値が存在しない場合, `0` 返されます。

例:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` ゼロ以上の引数のリストは、それぞれ文字列または整数のいずれかになります。

-   String=キ
-   正の整数=最初からn番目のメンバー/キーにアクセスします。
-   負の整数=最後からn番目のメンバー/キーにアクセスします。

要素の最小インデックスは1です。 したがって、要素0は存在しません。

整数を使用してJSON配列とJSONオブジェクトの両方にアクセスできます。

例えば:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices\_or\_keys\]…) {#jsonlengthjson-indices-or-keys}

JSON配列またはJSONオブジェクトの長さを返します。

値が存在しない場合、または型が間違っている場合, `0` 返されます。

例:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

JSON値の型を返します。

値が存在しない場合, `Null` 返されます。

例:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices\_or\_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices\_or\_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices\_or\_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices\_or\_keys\]…) {#jsonextractbooljson-indices-or-keys}

JSONを解析して値を抽出します。 これらの機能と類似 `visitParam` 機能。

値が存在しない場合、または型が間違っている場合, `0` 返されます。

例:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

JSONを解析して文字列を抽出します。 この関数は次のようになります `visitParamExtractString` 機能。

値が存在しないか、型が間違っている場合は、空の文字列が返されます。

値はエスケープされていません。 エスケープ解除に失敗した場合は、空の文字列を返します。

例:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices\_or\_keys…\], Return\_type) {#jsonextractjson-indices-or-keys-return-type}

JSONを解析し、指定されたClickHouseデータ型の値を抽出します。

これは以前のものの一般化です `JSONExtract<type>` 機能。
つまり
`JSONExtract(..., 'String')` とまったく同じを返します `JSONExtractString()`,
`JSONExtract(..., 'Float64')` とまったく同じを返します `JSONExtractFloat()`.

例:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices\_or\_keys…\], Value\_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

値が指定されたClickHouseデータ型であるJSONからキーと値のペアを解析します。

例:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)]
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

JSONの一部を解析されていない文字列として返します。

パーツが存在しないか、型が間違っている場合は、空の文字列が返されます。

例:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys…\]) {#jsonextractarrayrawjson-indices-or-keys}

JSON配列の要素を持つ配列を返します。

パーツが存在しないか配列でない場合は、空の配列が返されます。

例:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

## JSONExtractKeysAndValuesRaw {#json-extract-keys-and-values-raw}

JSONオブジェクトから生データを抽出します。

**構文**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**パラメータ**

-   `json` — [文字列](../data-types/string.md) 有効なJSONで。
-   `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [文字列](../data-types/string.md) キーまたはキーでフィールドを取得するには [整数](../data-types/int-uint.md) N番目のフィールドを取得するには（1からインデックス付けされ、負の整数は最後から数えます）。 設定されていない場合、JSON全体がトップレベルのオブジェクトとして解析されます。 任意パラメータ。

**戻り値**

-   との配列 `('key', 'value')` タプル 両方のタプルメンバーは文字列です。
-   要求されたオブジェクトが存在しない場合、または入力JSONが無効な場合は空の配列。

タイプ: [配列](../data-types/array.md)([タプル](../data-types/tuple.md)([文字列](../data-types/string.md), [文字列](../data-types/string.md)).

**例**

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
