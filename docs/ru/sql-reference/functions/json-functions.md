---
sidebar_position: 56
sidebar_label: JSON
---

# Функции для работы с JSON {#funktsii-dlia-raboty-s-json}

В Яндекс.Метрике пользователями передаётся JSON в качестве параметров визитов. Для работы с таким JSON-ом, реализованы некоторые функции. (Хотя в большинстве случаев, JSON-ы дополнительно обрабатываются заранее, и полученные значения кладутся в отдельные столбцы в уже обработанном виде.) Все эти функции исходят из сильных допущений о том, каким может быть JSON, и при этом стараются почти ничего не делать.

Делаются следующие допущения:

1.  Имя поля (аргумент функции) должно быть константой;
2.  Считается, что имя поля в JSON-е закодировано некоторым каноническим образом. Например, `visitParamHas('{"abc":"def"}', 'abc') = 1`, но `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Поля ищутся на любом уровне вложенности, без разбора. Если есть несколько подходящих полей - берётся первое.
4.  В JSON-е нет пробельных символов вне строковых литералов.

## visitParamHas(params, name) {#visitparamhasparams-name}

Проверяет наличие поля с именем `name`.

Синоним: `simpleJSONHas`.

## visitParamExtractUInt(params, name) {#visitparamextractuintparams-name}

Пытается выделить число типа UInt64 из значения поля с именем `name`. Если поле строковое, пытается выделить число из начала строки. Если такого поля нет, или если оно есть, но содержит не число, то возвращает 0.

Синоним: `simpleJSONExtractUInt`.

## visitParamExtractInt(params, name) {#visitparamextractintparams-name}

Аналогично для Int64.

Синоним: `simpleJSONExtractInt`.

## visitParamExtractFloat(params, name) {#visitparamextractfloatparams-name}

Аналогично для Float64.

Синоним: `simpleJSONExtractFloat`.

## visitParamExtractBool(params, name) {#visitparamextractboolparams-name}

Пытается выделить значение true/false. Результат — UInt8.

Синоним: `simpleJSONExtractBool`.

## visitParamExtractRaw(params, name) {#visitparamextractrawparams-name}

Возвращает значение поля, включая разделители.

Синоним: `simpleJSONExtractRaw`.

Примеры:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"';
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}';
```

## visitParamExtractString(params, name) {#visitparamextractstringparams-name}

Разбирает строку в двойных кавычках. У значения убирается экранирование. Если убрать экранированные символы не удалось, то возвращается пустая строка.

Синоним: `simpleJSONExtractString`.

Примеры:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0';
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺';
visitParamExtractString('{"abc":"\\u263"}', 'abc') = '';
visitParamExtractString('{"abc":"hello}', 'abc') = '';
```

На данный момент не поддерживаются записанные в формате `\uXXXX\uYYYY` кодовые точки не из basic multilingual plane (они переводятся не в UTF-8, а в CESU-8).

Следующие функции используют [simdjson](https://github.com/lemire/simdjson), который разработан под более сложные требования для разбора JSON. Упомянутое выше допущение 2 по-прежнему применимо.

## isValidJSON(json) {#isvalidjsonjson}

Проверяет, является ли переданная строка валидным json значением.

Примеры:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices_or_keys\]…) {#jsonhasjson-indices-or-keys}

Если значение существует в документе JSON, то возвращается `1`.

Если значение не существует, то возвращается `0`.

Примеры:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` — это список из нуля или более аргументов каждый из них может быть либо строкой либо целым числом.

-   Строка — это доступ к объекту по ключу.
-   Положительное целое число — это доступ к n-му члену/ключу с начала.
-   Отрицательное целое число — это доступ к n-му члену/ключу с конца.

Адресация элементов по индексу начинается с 1, следовательно элемент 0 не существует.

Вы можете использовать целые числа, чтобы адресовать как массивы JSON, так и JSON-объекты.

Примеры:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices_or_keys\]…) {#jsonlengthjson-indices-or-keys}

Возвращает длину массива JSON или объекта JSON.

Если значение не существует или имеет неверный тип, то возвращается `0`.

Примеры:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices_or_keys\]…) {#jsontypejson-indices-or-keys}

Возвращает тип значения JSON.

Если значение не существует, то возвращается `Null`.

Примеры:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices_or_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices_or_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices_or_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices_or_keys\]…) {#jsonextractbooljson-indices-or-keys}

Парсит JSON и извлекает значение. Эти функции аналогичны функциям `visitParam`.

Если значение не существует или имеет неверный тип, то возвращается `0`.

Примеры:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices_or_keys\]…) {#jsonextractstringjson-indices-or-keys}

Парсит JSON и извлекает строку. Эта функция аналогична функции `visitParamExtractString`.

Если значение не существует или имеет неверный тип, то возвращается пустая строка.

У значения убирается экранирование. Если убрать экранированные символы не удалось, то возвращается пустая строка.

Примеры:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices_or_keys…\], Return_type) {#jsonextractjson-indices-or-keys-return-type}

Парсит JSON и извлекает значение с заданным типом данных.

Это обобщение предыдущих функций `JSONExtract<type>`.
Это означает
`JSONExtract(..., 'String')` выдает такой же результат, как `JSONExtractString()`,
`JSONExtract(..., 'Float64')` выдает такой же результат, как `JSONExtractFloat()`.

Примеры:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices_or_keys…\], Value_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Разбор пар ключ-значение из JSON, где значение имеет тип данных ClickHouse.

Пример:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## JSONExtractKeys {#jsonextractkeysjson-indices-or-keys}

Парсит строку JSON и извлекает ключи.

**Синтаксис**

``` sql
JSONExtractKeys(json[, a, b, c...])
```

**Аргументы**

-   `json` — [строка](../data-types/string.md), содержащая валидный JSON.
-   `a, b, c...` — индексы или ключи, разделенные запятыми, которые указывают путь к внутреннему полю во вложенном объекте JSON. Каждый аргумент может быть либо [строкой](../data-types/string.md) для получения поля по ключу, либо [целым числом](../data-types/int-uint.md) для получения N-го поля (индексирование начинается с 1, отрицательные числа используются для отсчета с конца). Если параметр не задан, весь JSON разбирается как объект верхнего уровня. Необязательный параметр.

**Возвращаемые значения**

Массив с ключами JSON.

Тип: [Array](../data-types/array.md)([String](../data-types/string.md)). 

**Пример**

Запрос:

```sql
SELECT JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}');
```

Результат:

```
text
┌─JSONExtractKeys('{"a": "hello", "b": [-100, 200.0, 300]}')─┐
│ ['a','b']                                                  │
└────────────────────────────────────────────────────────────┘
```

## JSONExtractRaw(json\[, indices_or_keys\]…) {#jsonextractrawjson-indices-or-keys}

Возвращает часть JSON в виде строки, содержащей неразобранную подстроку.

Если значение не существует, то возвращается пустая строка.

Пример:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]';
```

## JSONExtractArrayRaw(json\[, indices_or_keys\]…) {#jsonextractarrayrawjson-indices-or-keys}

Возвращает массив из элементов JSON массива, каждый из которых представлен в виде строки с неразобранными подстроками из JSON.

Если значение не существует или не является массивом, то возвращается пустой массив.

Пример:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']';
```

## JSONExtractKeysAndValuesRaw {#json-extract-keys-and-values-raw}

Извлекает необработанные данные из объекта JSON.

**Синтаксис**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Аргументы**

-   `json` — [строка](../data-types/string.md), содержащая валидный JSON.
-   `p, a, t, h` — индексы или ключи, разделенные запятыми, которые указывают путь к внутреннему полю во вложенном объекте JSON. Каждый аргумент может быть либо [строкой](../data-types/string.md) для получения поля по ключу, либо [целым числом](../data-types/int-uint.md) для получения N-го поля (индексирование начинается с 1, отрицательные числа используются для отсчета с конца). Если параметр не задан, весь JSON парсится как объект верхнего уровня. Необязательный параметр.

**Возвращаемые значения**

-   Массив с кортежами `('key', 'value')`. Члены кортежа — строки.

-   Пустой массив, если заданный объект не существует или входные данные не валидный JSON.

Тип: [Array](../data-types/array.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md), [String](../data-types/string.md)).

**Примеры**

Запрос:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}');
```

Результат:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b');
```

Результат:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c');
```

Результат:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## JSON_EXISTS(json, path) {#json-exists}

Если значение существует в документе JSON, то возвращается 1.

Если значение не существует, то возвращается 0.

Пример:

``` sql
SELECT JSON_EXISTS('{"hello":1}', '$.hello');
SELECT JSON_EXISTS('{"hello":{"world":1}}', '$.hello.world');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[*]');
SELECT JSON_EXISTS('{"hello":["world"]}', '$.hello[0]');
```

    :::note "Примечание"
    до версии 21.11 порядок аргументов функции был обратный, т.е. JSON_EXISTS(path, json)
    :::
## JSON_QUERY(json, path) {#json-query}

Парсит JSON и извлекает значение как JSON массив или JSON объект.

Если значение не существует, то возвращается пустая строка.

Пример:

``` sql
SELECT JSON_QUERY('{"hello":"world"}', '$.hello');
SELECT JSON_QUERY('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_QUERY('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_QUERY('{"hello":2}', '$.hello'));
```

Результат:

``` text
["world"]
[0, 1, 4, 0, -1, -4]
[2]
String
```
    :::note "Примечание"
    до версии 21.11 порядок аргументов функции был обратный, т.е. JSON_QUERY(path, json)
    :::
## JSON_VALUE(json, path) {#json-value}

Парсит JSON и извлекает значение как JSON скаляр.

Если значение не существует, то возвращается пустая строка.

Пример:

``` sql
SELECT JSON_VALUE('{"hello":"world"}', '$.hello');
SELECT JSON_VALUE('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');
SELECT JSON_VALUE('{"hello":2}', '$.hello');
SELECT toTypeName(JSON_VALUE('{"hello":2}', '$.hello'));
```

Результат:

``` text
"world"
0
2
String
```

    :::note "Примечание"
    до версии 21.11 порядок аргументов функции был обратный, т.е. JSON_VALUE(path, json)
    :::
## toJSONString {#tojsonstring}

Сериализует значение в JSON представление. Поддерживаются различные типы данных и вложенные структуры.
По умолчанию 64-битные [целые числа](../../sql-reference/data-types/int-uint.md) и более (например, `UInt64` или `Int128`) заключаются в кавычки. Настройка [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) управляет этим поведением.
Специальные значения `NaN` и `inf` заменяются на `null`. Чтобы они отображались, включите настройку [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals).
Когда сериализуется значение [Enum](../../sql-reference/data-types/enum.md), то функция выводит его имя.

**Синтаксис**

``` sql
toJSONString(value)
```

**Аргументы**

-   `value` — значение, которое необходимо сериализовать. Может быть любого типа.

**Возвращаемое значение**

-   JSON представление значения.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Первый пример показывает сериализацию [Map](../../sql-reference/data-types/map.md).
Во втором примере есть специальные значения, обернутые в [Tuple](../../sql-reference/data-types/tuple.md).

Запрос:

``` sql
SELECT toJSONString(map('key1', 1, 'key2', 2));
SELECT toJSONString(tuple(1.25, NULL, NaN, +inf, -inf, [])) SETTINGS output_format_json_quote_denormals = 1;
```

Результат:

``` text
{"key1":1,"key2":2}
[1.25,null,"nan","inf","-inf",[]]
```

**Смотрите также**

-   [output_format_json_quote_64bit_integers](../../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers)
-   [output_format_json_quote_denormals](../../operations/settings/settings.md#settings-output_format_json_quote_denormals)
