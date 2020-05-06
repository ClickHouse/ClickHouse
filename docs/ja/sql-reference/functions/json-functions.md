---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 56
toc_title: "JSON\u3067\u306E\u4F5C\u696D."
---

# JSONを操作するための関数 {#functions-for-working-with-json}

Yandexの中。Metrica、JSONで送信したユーザーとしてセッションパラメータ。 このJSONを操作するための特別な関数がいくつかあります。 （ほとんどの場合、JSONsはさらに前処理され、結果の値は処理された形式で別々の列に格納されます。）これらの関数はすべて、JSONができることについての強い前提に基づいていますが、仕事を終わらせるためにできるだけ少なくしようとします。

以下の仮定が行われます:

1.  フィールド名(関数の引数)は定数でなければなりません。
2.  フィールド名は何とかcanonicallyで符号化されたjson. 例えば: `visitParamHas('{"abc":"def"}', 'abc') = 1`、しかし `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  フィールドは、任意の入れ子レベルで無差別に検索されます。 一致するフィールドが複数ある場合は、最初のオカレンスが使用されます。
4.  JSONには、文字列リテラルの外側にスペース文字はありません。

## visitParamHas(パラメータ,名前) {#visitparamhasparams-name}

フィールドがあるかどうかをチェック ‘name’ 名前だ

## visitParamExtractUInt(パラメータ,名前) {#visitparamextractuintparams-name}

指定されたフィールドの値からuint64を解析します ‘name’. これが文字列フィールドの場合、文字列の先頭から数値を解析しようとします。 フィールドが存在しないか、存在するが数値が含まれていない場合は、0を返します。

## visitParamExtractInt(パラメータ,名前) {#visitparamextractintparams-name}

Int64の場合と同じです。

## visitParamExtractFloat(パラメーター,名前) {#visitparamextractfloatparams-name}

Float64の場合と同じです。

## visitParamExtractBool(パラメーター,名前) {#visitparamextractboolparams-name}

True/false値を解析します。 結果はUInt8です。

## visitParamExtractRaw(パラメータ,名前) {#visitparamextractrawparams-name}

セパレータを含むフィールドの値を返します。

例:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(パラメーター,名前) {#visitparamextractstringparams-name}

文字列を二重引用符で解析します。 値はエスケープされません。 エスケープ解除に失敗した場合は、空の文字列を返します。

例:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

現在、この形式のコードポイントはサポートされていません `\uXXXX\uYYYY` これは、（彼らはCESU-8の代わりにUTF-8に変換されます）基本的な多言語面からではありません。

次の関数は、以下に基づいています [simdjson](https://github.com/lemire/simdjson) より複雑なJSON解析要件のために設計。 上記の前提2は依然として適用されます。

## isValidJSON(json) {#isvalidjsonjson}

渡された文字列が有効なjsonであることを確認します。

例:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

JSONドキュメントに値が存在する場合, `1` は返却されます。

値が存在しない場合, `0` は返却されます。

例:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` それぞれの引数は、文字列または整数のいずれかになります。

-   文字列=アクセスオブジェクトにより、会員に対す。
-   正の整数=最初からn番目のメンバー/キーにアクセスします。
-   負の整数=最後からn番目のメンバー/キーにアクセスします。

要素の最小インデックスは1です。 したがって、要素0は存在しません。

整数を使用して、json配列とjsonオブジェクトの両方にアクセスできます。

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

値が存在しないか、間違った型を持っている場合, `0` は返却されます。

例:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

JSON値の型を返します。

値が存在しない場合, `Null` は返却されます。

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

JSONを解析し、値を抽出します。 これらの機能と類似 `visitParam` 機能。

値が存在しないか、間違った型を持っている場合, `0` は返却されます。

例:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

JSONを解析し、文字列を抽出します。 この関数は次のようになります `visitParamExtractString` 機能。

値が存在しないか、間違った型を持っている場合は、空の文字列が返されます。

値はエスケープされません。 エスケープ解除に失敗した場合は、空の文字列を返します。

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

これは以前の一般化です `JSONExtract<type>` 機能。
これは
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

値が指定されたclickhouseデータ型のjsonからキーと値のペアを解析します。

例えば:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

JSONの一部を返します。

パートが存在しないか、間違った型を持っている場合は、空の文字列が返されます。

例えば:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys\]…) {#jsonextractarrayrawjson-indices-or-keys}

それぞれが未解析の文字列として表されるjson配列の要素を持つ配列を返します。

その部分が存在しない場合、または配列でない場合は、空の配列が返されます。

例えば:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
