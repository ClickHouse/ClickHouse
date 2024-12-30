---
slug: /ja/sql-reference/functions/json-functions
sidebar_position: 105
sidebar_label: JSON
---

JSONを解析するための関数は2つのセットがあります：
   - 限定されているサブセットのJSONを極めて高速に解析するための[`simpleJSON*` (`visitParam*`)](#simplejson-visitparam-functions)。
   - 通常のJSONを解析するための[`JSONExtract*`](#jsonextract-functions)。

## simpleJSON (visitParam) 関数

ClickHouseには、簡易化されたJSONを操作するための特別な関数があります。これらのJSON関数は、JSONがどのようなものであるかについて強力な仮定に基づいています。可能な限り少ない労力で、できるだけ早く作業を完了することを目指しています。

次の仮定がされています：

1. フィールド名（関数の引数）は定数でなければなりません。
2. フィールド名は、JSONで標準的にエンコードされている必要があります。例えば、`simpleJSONHas('{"abc":"def"}', 'abc') = 1` ですが、`simpleJSONHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3. フィールドは、どのネストレベルでも無差別に検索されます。複数の一致するフィールドがある場合、最初の出現が使用されます。
4. JSONは文字列リテラル外にスペース文字を持ちません。

### simpleJSONHas

`field_name`という名前のフィールドがあるかどうかをチェックします。結果は`UInt8`です。

**構文**

```sql
simpleJSONHas(json, field_name)
```

別名: `visitParamHas`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドが存在する場合は`1`を返し、存在しない場合は`0`を返します。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'bar') FROM jsons;
```

結果:

```response
1
0
```
### simpleJSONExtractUInt

`field_name`という名前のフィールドの値から`UInt64`を解析します。文字列フィールドの場合、文字列の先頭から数値を解析しようとします。フィールドが存在しない場合、または数値を含まない場合、`0`を返します。

**構文**

```sql
simpleJSONExtractUInt(json, field_name)
```

別名: `visitParamExtractUInt`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドが存在し、数値を含む場合はフィールドから解析された数を返し、それ以外の場合は`0`を返します。[UInt64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"4e3"}');
INSERT INTO jsons VALUES ('{"foo":3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractUInt(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response
0
4
0
3
5
```

### simpleJSONExtractInt

`field_name`という名前のフィールドの値から`Int64`を解析します。文字列フィールドの場合は、文字列の先頭から数値を解析しようとします。フィールドが存在しない場合、または数値を含まない場合、`0`を返します。

**構文**

```sql
simpleJSONExtractInt(json, field_name)
```

別名: `visitParamExtractInt`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドが存在し、数値を含む場合はフィールドから解析された数を返し、それ以外の場合は`0`を返します。[Int64](../data-types/int-uint.md)。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractInt(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response
0
-4
0
-3
5
```

### simpleJSONExtractFloat

`field_name`という名前のフィールドの値から`Float64`を解析します。文字列フィールドの場合は、文字列の先頭から数値を解析しようとします。フィールドが存在しない場合、または数値を含まない場合、`0`を返します。

**構文**

```sql
simpleJSONExtractFloat(json, field_name)
```

別名: `visitParamExtractFloat`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドが存在し、数値を含む場合はフィールドから解析された数を返し、それ以外の場合は`0`を返します。[Float64](../data-types/float.md/#float32-float64)。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":"not1number"}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractFloat(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response
0
-4000
0
-3.4
5
```

### simpleJSONExtractBool

`field_name`という名前のフィールドの値から真偽値を解析します。結果は`UInt8`です。

**構文**

```sql
simpleJSONExtractBool(json, field_name)
```

別名: `visitParamExtractBool`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

フィールドの値が`true`の場合は`1`を、それ以外は`0`を返します。したがって、次の場合を含め（これらに限定されない）この関数は`0`を返します：
 - フィールドが存在しない場合。
 - フィールドが文字列として`true`を含む場合、例: `{"field":"true"}`。
 - フィールドが数値値として`1`を含む場合。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":false,"bar":true}');
INSERT INTO jsons VALUES ('{"foo":"true","qux":1}');

SELECT simpleJSONExtractBool(json, 'bar') FROM jsons ORDER BY json;
SELECT simpleJSONExtractBool(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response
0
1
0
0
```

### simpleJSONExtractRaw

`field_name`という名前のフィールドの値をセパレータを含む`String`として返します。

**構文**

```sql
simpleJSONExtractRaw(json, field_name)
```

別名: `visitParamExtractRaw`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドが存在する場合は値をセパレータを含む文字列として返し、存在しない場合は空の文字列を返します。[`String`](../data-types/string.md#string)

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"-4e3"}');
INSERT INTO jsons VALUES ('{"foo":-3.4}');
INSERT INTO jsons VALUES ('{"foo":5}');
INSERT INTO jsons VALUES ('{"foo":{"def":[1,2,3]}}');
INSERT INTO jsons VALUES ('{"baz":2}');

SELECT simpleJSONExtractRaw(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response

"-4e3"
-3.4
5
{"def":[1,2,3]}
```

### simpleJSONExtractString

ダブルクォート付きの`String`を`field_name`という名前のフィールドの値から解析します。

**構文**

```sql
simpleJSONExtractString(json, field_name)
```

別名: `visitParamExtractString`.

**パラメータ**

- `json` — フィールドが検索されるJSON。[String](../data-types/string.md#string)
- `field_name` — 検索されるフィールドの名前。[String literal](../syntax#string)

**返される値**

- フィールドの値をセパレータを含めて文字列として返します。フィールドがダブルクォート付き文字列を含まない場合、エスケープに失敗した場合、またはフィールドが存在しない場合は空の文字列を返します。[String](../data-types/string.md)。

**実装の詳細**

現在、基本多言語面に含まれない形式の`\\uXXXX\\uYYYY`に対応するコードポイントのサポートはありません（CESU-8に変換されますが、UTF-8には変換されません）。

**例**

クエリ:

```sql
CREATE TABLE jsons
(
    `json` String
)
ENGINE = Memory;

INSERT INTO jsons VALUES ('{"foo":"\\n\\u0000"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263"}');
INSERT INTO jsons VALUES ('{"foo":"\\u263a"}');
INSERT INTO jsons VALUES ('{"foo":"hello}');

SELECT simpleJSONExtractString(json, 'foo') FROM jsons ORDER BY json;
```

結果:

```response
\n\0

☺

```

## JSONExtract 関数

次の関数は[simdjson](https://github.com/lemire/simdjson)に基づいており、より複雑なJSONの解析要件に対応しています。

### isValidJSON

渡された文字列が有効なJSONかどうかをチェックします。

**構文**

```sql
isValidJSON(json)
```

**例**

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

### JSONHas

JSONドキュメントに値が存在する場合は`1`を返します。値が存在しない場合は`0`を返します。

**構文**

```sql
JSONHas(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- `json`に値が存在する場合は`1`、それ以外の場合は`0`を返します。[UInt8](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

要素の最小インデックスは1です。そのため、要素0は存在しません。整数を使ってJSON配列およびJSONオブジェクトにアクセスできます。例えば：

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

### JSONLength

JSON配列またはJSONオブジェクトの長さを返します。値が存在しない、または間違った型の場合、`0`を返します。

**構文**

```sql
JSONLength(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- JSON配列またはJSONオブジェクトの長さを返します。値が存在しない、または不正な型の場合、`0`を返します。[UInt64](../data-types/int-uint.md)。

**例**

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

### JSONType

JSON値の型を返します。値が存在しない場合、`Null`が返されます。

**構文**

```sql
JSONType(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- JSON値の型を文字列として返します。値が存在しない場合は`Null`を返します。[String](../data-types/string.md)。

**例**

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

### JSONExtractUInt

JSONを解析してUInt型の値を抽出します。

**構文**

```sql
JSONExtractUInt(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- UInt値が存在する場合はそれを返し、存在しない場合は`Null`を返します。[UInt64](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) as x, toTypeName(x);
```

結果:

```response
┌───x─┬─toTypeName(x)─┐
│ 300 │ UInt64        │
└─────┴───────────────┘
```

### JSONExtractInt

JSONを解析してInt型の値を抽出します。

**構文**

```sql
JSONExtractInt(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- Int値が存在する場合はそれを返し、存在しない場合は`Null`を返します。[Int64](../data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) as x, toTypeName(x);
```

結果:

```response
┌───x─┬─toTypeName(x)─┐
│ 300 │ Int64         │
└─────┴───────────────┘
```

### JSONExtractFloat

JSONを解析してInt型の値を抽出します。

**構文**

```sql
JSONExtractFloat(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- Float値が存在する場合はそれを返し、存在しない場合は`Null`を返します。[Float64](../data-types/float.md)。

**例**

クエリ:

``` sql
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) as x, toTypeName(x);
```

結果:

```response
┌───x─┬─toTypeName(x)─┐
│ 200 │ Float64       │
└─────┴───────────────┘
```

### JSONExtractBool

JSONを解析してブール値を抽出します。値が存在しないか間違った型の場合、`0`が返されます。

**構文**

```sql
JSONExtractBool(json\[, indices_or_keys\]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- ブール値が存在する場合はそれを返し、存在しない場合は`0`を返します。[Bool](../data-types/boolean.md)。

**例**

クエリ:

``` sql
SELECT JSONExtractBool('{"passed": true}', 'passed');
```

結果:

```response
┌─JSONExtractBool('{"passed": true}', 'passed')─┐
│                                             1 │
└───────────────────────────────────────────────┘
```

### JSONExtractString

JSONを解析して文字列を抽出します。この関数は[`visitParamExtractString`](#simplejsonextractstring)関数と似ています。値が存在しないか間違った型の場合、空の文字列が返されます。

**構文**

```sql
JSONExtractString(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md).

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- `json`からエスケープ解除された文字列を返します。エスケープ解除に失敗した場合、値が存在しないか間違った型の場合は空の文字列を返します。[String](../data-types/string.md)。

**例**

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

### JSONExtract

解析されたJSONから指定したClickHouseのデータ型の値を抽出します。この関数は以前の`JSONExtract<type>`関数の一般化バージョンです。つまり：

`JSONExtract(..., 'String')` は `JSONExtractString()`と全く同じものを返します。
`JSONExtract(..., 'Float64')` は `JSONExtractFloat()`と全く同じものを返します。

**構文**

```sql
JSONExtract(json [, indices_or_keys...], return_type)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md).
- `return_type` — 抽出する値の型を指定する文字列。[String](../data-types/string.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- 指定された返却型の値が存在すればそれを返し、存在しない場合は指定された返却型に応じて`0`、`Null`、または空の文字列を返します。 [UInt64](../data-types/int-uint.md), [Int64](../data-types/int-uint.md), [Float64](../data-types/float.md), [Bool](../data-types/boolean.md)または[String](../data-types/string.md)。

**例**

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": "world"}', 'Map(String, String)') = map('a',  'hello', 'b', 'world');
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

### JSONExtractKeysAndValues

与えられたClickHouseデータ型の値を持つキー-値ペアをJSONから解析します。

**構文**

```sql
JSONExtractKeysAndValues(json [, indices_or_keys...], value_type)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md).
- `value_type` — 抽出する値の型を指定する文字列。[String](../data-types/string.md)。

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- 解析されたキー-値ペアの配列を返します。[Array](../data-types/array.md)([Tuple](../data-types/tuple.md)(`value_type`))。

**例**

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

### JSONExtractKeys

JSON文字列を解析してキーを抽出します。

**構文**

``` sql
JSONExtractKeys(json[, a, b, c...])
```

**パラメータ**

- `json` — 有効なJSONを含む[String](../data-types/string.md)。
- `a, b, c...` — ネストされたJSONオブジェクト内で内側のフィールドへのパスを指定するカンマ区切りのインデックスまたはキー。各引数は、キーによるフィールド取得に使う[String](../data-types/string.md)またはN番目のフィールド取得に使う[Integer](../data-types/int-uint.md)。指定しない場合は、全体のJSONがトップレベルオブジェクトとして解析されます。オプションのパラメータ。

**返される値**

- JSONのキーの配列を返します。[Array](../data-types/array.md)([String](../data-types/string.md))。

**例**

クエリ:

```sql
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}');
```

結果:

```
text
┌─JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}')─┐
│ ['a','b']                                                  │
└────────────────────────────────────────────────────────────┘
```

### JSONExtractRaw

未解析の文字列としてJSONの一部を返します。部分が存在しないか間違った型の場合、空の文字列が返されます。

**構文**

```sql
JSONExtractRaw(json [, indices_or_keys]...)
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md].

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- 未解析の文字列としてJSONの一部を返します。部分が存在しないか間違った型の場合、空の文字列が返されます。[String](../data-types/string.md)。

**例**

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]';
```

### JSONExtractArrayRaw

JSON配列の各要素を未解析の文字列として持つ配列を返します。部分が存在しないか配列でない場合は、空の配列が返されます。

**構文**

```sql
JSONExtractArrayRaw(json [, indices_or_keys...])
```

**パラメータ**

- `json` — 解析するJSON文字列。[String](../data-types/string.md)。
- `indices_or_keys` — ゼロまたはそれ以上の引数のリストで、それぞれが文字列または整数です。[String](../data-types/string.md), [Int*](../data-types/int-uint.md].

`indices_or_keys` のタイプ:
- 文字列 = キーによるオブジェクトメンバーへのアクセス。
- 正の整数 = 開始からn番目のメンバー/キーへのアクセス。
- 負の整数 = 終わりからn番目のメンバー/キーへのアクセス。

**返される値**

- 各要素が未解析の文字列として表されるJSON配列の要素を持つ配列を返します。それ以外の場合は、部分が存在しないか配列でない場合は空の配列を返します。[Array](../data-types/array.md)([String](../data-types/string.md))。

**例**

```sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"'];
```

### JSONExtractKeysAndValuesRaw

JSONオブジェクトからの生データを抽出します。

**構文**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**引数**

- `json` — 有効なJSONを含む[String](../data-types/string.md)。
- `p, a, t, h` — ネストされたJSONオブジェクト内で内側のフィールドへのパスを指定するカンマ区切りのインデックスまたはキー。各引数は、キーによるフィールド取得に使う[string](../data-types/string.md)またはN番目のフィールド取得に使う[integer](../data-types/int-uint.md)。指定しない場合は、全体のJSONがトップレベルオブジェクトとして解析されます。オプションのパラメータ。

**返される値**

- `('key', 'value')`タプルの配列。タプルの両要素は文字列です。[Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), [String](../data-types/string.md))。
- 要求されたオブジェクトが存在しない場合、または入力JSONが無効な場合は空の配列。[Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), [String](../data-types/string.md))。

**例**

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}');
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b');
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

クエリ:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c');
```

結果:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### JSON_EXISTS

JSONドキュメントに値が存在する場合は`1`を返します。値が存在しない場合は`0`を返します。

**構文**

```sql
JSON_EXISTS(json, path)
```

**パラメータ**

- `json` — 有効なJSONを含む文字列。[String](../data-types/string.md)。 
- `path` — パスを表す文字列。[String](../data-types/string.md)。

:::note
バージョン21.11より前だと引数の順序が誤っていました。つまり、JSON_EXISTS(path, json)
:::

**返される値**

- JSONドキュメントに値が存在すれば`1`、それ以外は`0`を返します。

**例**

``` sql
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
```

### JSON_QUERY

JSONを解析してJSON配列またはJSONオブジェクトとして値を抽出します。値が存在しない場合は、空の文字列が返されます。

**構文**

```sql
JSON_QUERY(json, path)
```

**パラメータ**

- `json` — 有効なJSONを含む文字列。[String](../data-types/string.md)。 
- `path` — パスを表す文字列。[String](../data-types/string.md)。

:::note
バージョン21.11より前だと引数の順序が誤っていました。つまり、JSON_EXISTS(path, json)
:::

**返される値**

- 抽出された値をJSON配列またはJSONオブジェクトとして返します。それ以外の場合、値が存在しない場合は空の文字列を返します。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT JSON_QUERY('{"hello":"world"}', '$.hello');
SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_QUERY('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_QUERY('{"hello":2}', '$.hello'));
```

結果:

``` text
["world"]
[0, 1, 4, 0, -1, -4]
[2]
String
```

### JSON_VALUE

JSONを解析してJSONスカラーとして値を抽出します。値が存在しない場合は、デフォルトで空の文字列が返されます。

この関数は次の設定によって制御されます：

- `function_json_value_return_type_allow_nullable` を `true`に設定することで、`NULL`が返されます。値が複雑型（構造体、配列、マップのような）の場合、デフォルトで空の文字列が返されます。
- `function_json_value_return_type_allow_complex` を `true`に設定することで、複雑な値が返されます。

**構文**

```sql
JSON_VALUE(json, path)
```

**パラメータ**

- `json` — 有効なJSONを含む文字列。[String](../data-types/string.md)。 
- `path` — パスを表す文字列。[String](../data-types/string.md)。

:::note
バージョン21.11より前だと引数の順序が誤っていました。つまり、JSON_EXISTS(path, json)
:::

**返される値**

- JSONスカラーとして抽出された値を返します。存在しない場合は空の文字列を返します。[String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT JSON_VALUE('{"hello":"world"}', '$.hello');
SELECT JSON_VALUE('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_VALUE('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_VALUE('{"hello":2}', '$.hello'));
select JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;
select JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true;
```

結果:

``` text
world
0
2
String
```

### toJSONString

値をそのJSON表現にシリアル化します。さまざまなデータ型やネストされた構造がサポートされます。
デフォルトで、64ビットの[整数](../data-types/int-uint.md)以上のもの（例： `UInt64` または `Int128`）は引用符で囲まれます。[output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers)がこの動作を制御します。
特別な値`NaN` と `inf` は `null` に置き換えられます。これを表示するには、[output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals)設定を有効にします。
[Enum](../data-types/enum.md)値をシリアル化すると、その名前が出力されます。

**構文**

``` sql
toJSONString(value)
```

**引数**

- `value` — シリアル化する値。値は任意のデータ型である可能性があります。

**返される値**

- 値のJSON表現。[String](../data-types/string.md)。

**例**

最初の例は[Map](../data-types/map.md)のシリアル化を示しています。
2つ目の例は、[Tuple](../data-types/tuple.md)にラップされた特別な値を示しています。

クエリ:

``` sql
SELECT toJSONString(map('key1', 1, 'key2', 2));
SELECT toJSONString(tuple(1.25, NULL, NaN, +inf, -inf, [])) SETTINGS output_format_json_quote_denormals = 1;
```

結果:

``` text
{"key1":1,"key2":2}
[1.25,null,"nan","inf","-inf",[]]
```

**関連リンク**

- [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers)
- [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals)


### JSONArrayLength

最も外側のJSON配列の要素数を返します。入力されたJSON文字列が無効な場合、関数はNULLを返します。

**構文**

``` sql
JSONArrayLength(json)
```

別名: `JSON_ARRAY_LENGTH(json)`。

**引数**

- `json` — 有効なJSONを含む[String](../data-types/string.md)。

**返される値**

- `json`が有効なJSON配列文字列であれば、配列要素の数を返し、それ以外の場合はNULLを返します。[Nullable(UInt64)](../data-types/int-uint.md)。

**例**

``` sql
SELECT
    JSONArrayLength(''),
    JSONArrayLength('[1,2,3]')

┌─JSONArrayLength('')─┬─JSONArrayLength('[1,2,3]')─┐
│                ᴺᵁᴸᴸ │                          3 │
└─────────────────────┴────────────────────────────┘
```


### jsonMergePatch

複数のJSONオブジェクトをマージして形成されたマージ済みのJSONオブジェクト文字列を返します。

**構文**

``` sql
jsonMergePatch(json1, json2, ...)
```

**引数**

- `json` — 有効なJSONを含む[String](../data-types/string.md)。

**返される値**

- JSONオブジェクト文字列が有効であれば、マージ済みのJSONオブジェクト文字列を返します。[String](../data-types/string.md)。

**例**

``` sql
SELECT jsonMergePatch('{"a":1}', '{"name": "joey"}', '{"name": "tom"}', '{"name": "zoey"}') AS res

┌─res───────────────────┐
│ {"a":1,"name":"zoey"} │
└───────────────────────┘
```

### JSONAllPaths

[JSON](../data-types/newjson.md)カラム内の各行に保存されているすべてのパスのリストを返します。

**構文**

``` sql
JSONAllPaths(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Array(String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONAllPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a":"42"}                           │ ['a']              │
│ {"b":"Hello"}                        │ ['b']              │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['a','c']          │
└──────────────────────────────────────┴────────────────────┘
```

### JSONAllPathsWithTypes

[JSON](../data-types/newjson.md)カラム内の各行に保存されているすべてのパスとそれらのデータ型のマップを返します。

**構文**

``` sql
JSONAllPathsWithTypes(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Map(String, String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONAllPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONAllPathsWithTypes(json)───────────────┐
│ {"a":"42"}                           │ {'a':'Int64'}                             │
│ {"b":"Hello"}                        │ {'b':'String'}                            │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'a':'Array(Nullable(Int64))','c':'Date'} │
└──────────────────────────────────────┴───────────────────────────────────────────┘
```

### JSONDynamicPaths

[JSON](../data-types/newjson.md)カラム内に別々のサブカラムとして格納されている動的パスのリストを返します。

**構文**

``` sql
JSONDynamicPaths(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Array(String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONDynamicPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONDynamicPaths(json)─┐
| {"a":"42"}                           │ ['a']                  │
│ {"b":"Hello"}                        │ []                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['a']                  │
└──────────────────────────────────────┴────────────────────────┘
```

### JSONDynamicPathsWithTypes

[JSON](../data-types/newjson.md)カラム内で別々のサブカラムとして格納される動的パスとその型のマップを各行に返します。

**構文**

``` sql
JSONAllPathsWithTypes(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Map(String, String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONDynamicPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONDynamicPathsWithTypes(json)─┐
│ {"a":"42"}                           │ {'a':'Int64'}                   │
│ {"b":"Hello"}                        │ {}                              │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'a':'Array(Nullable(Int64))'}  │
└──────────────────────────────────────┴─────────────────────────────────┘
```

### JSONSharedDataPaths

[JSON](../data-types/newjson.md)カラム内の共有データ構造に保存されているパスのリストを返します。

**構文**

``` sql
JSONSharedDataPaths(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Array(String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONSharedDataPaths(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONSharedDataPaths(json)─┐
│ {"a":"42"}                           │ []                        │
│ {"b":"Hello"}                        │ ['b']                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ ['c']                     │
└──────────────────────────────────────┴───────────────────────────┘
```

### JSONSharedDataPathsWithTypes

[JSON](../data-types/newjson.md)カラム内で共有データ構造に保存されているパスとその型のマップを各行に返します。

**構文**

``` sql
JSONSharedDataPathsWithTypes(json)
```

**引数**

- `json` — [JSON](../data-types/newjson.md).

**返される値**

- パスの配列。[Map(String, String)](../data-types/array.md)。

**例**

``` sql
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json" : {"a" : 42}}, {"json" : {"b" : "Hello"}}, {"json" : {"a" : [1, 2, 3], "c" : "2020-01-01"}}
SELECT json, JSONSharedDataPathsWithTypes(json) FROM test;
```

```text
┌─json─────────────────────────────────┬─JSONSharedDataPathsWithTypes(json)─┐
│ {"a":"42"}                           │ {}                                 │
│ {"b":"Hello"}                        │ {'b':'String'}                     │
│ {"a":["1","2","3"],"c":"2020-01-01"} │ {'c':'Date'}                       │
└──────────────────────────────────────┴────────────────────────────────────┘
```
