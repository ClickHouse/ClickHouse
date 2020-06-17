---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_title: Trabajar con JSON
---

# Funciones para trabajar con JSON {#functions-for-working-with-json}

En el Yandex.Metrica, JSON es transmitido por los usuarios como parámetros de sesión. Hay algunas funciones especiales para trabajar con este JSON. (Aunque en la mayoría de los casos, los JSON también se procesan previamente, y los valores resultantes se colocan en columnas separadas en su formato procesado.) Todas estas funciones se basan en sólidas suposiciones sobre lo que puede ser el JSON, pero tratan de hacer lo menos posible para hacer el trabajo.

Se hacen las siguientes suposiciones:

1.  El nombre de campo (argumento de función) debe ser una constante.
2.  El nombre del campo de alguna manera está codificado canónicamente en JSON. Por ejemplo: `visitParamHas('{"abc":"def"}', 'abc') = 1`, pero `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  Los campos se buscan en cualquier nivel de anidación, indiscriminadamente. Si hay varios campos coincidentes, se utiliza la primera aparición.
4.  El JSON no tiene caracteres de espacio fuera de los literales de cadena.

## visitParamHas (params, nombre) {#visitparamhasparams-name}

Comprueba si hay un campo con el ‘name’ nombre.

## visitParamExtractUInt (params, nombre) {#visitparamextractuintparams-name}

Analiza UInt64 a partir del valor del campo denominado ‘name’. Si se trata de un campo de cadena, intenta analizar un número desde el principio de la cadena. Si el campo no existe, o existe pero no contiene un número, devuelve 0.

## visitParamExtractInt (params, nombre) {#visitparamextractintparams-name}

Lo mismo que para Int64.

## visitParamExtractFloat (params, nombre) {#visitparamextractfloatparams-name}

Lo mismo que para Float64.

## visitParamExtractBool (params, nombre) {#visitparamextractboolparams-name}

Analiza un valor verdadero/falso. El resultado es UInt8.

## visitParamExtractRaw (params, nombre) {#visitparamextractrawparams-name}

Devuelve el valor de un campo, incluidos los separadores.

Ejemplos:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## visitParamExtractString(params, nombre) {#visitparamextractstringparams-name}

Analiza la cadena entre comillas dobles. El valor es sin escape. Si no se pudo desescapar, devuelve una cadena vacía.

Ejemplos:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

Actualmente no hay soporte para puntos de código en el formato `\uXXXX\uYYYY` que no son del plano multilingüe básico (se convierten a CESU-8 en lugar de UTF-8).

Las siguientes funciones se basan en [simdjson](https://github.com/lemire/simdjson) diseñado para requisitos de análisis JSON más complejos. La suposición 2 mencionada anteriormente todavía se aplica.

## ¿Qué puedes encontrar en Neodigit) {#isvalidjsonjson}

Comprueba que la cadena pasada es un json válido.

Ejemplos:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices\_or\_keys\]…) {#jsonhasjson-indices-or-keys}

Si el valor existe en el documento JSON, `1` serán devueltos.

Si el valor no existe, `0` serán devueltos.

Ejemplos:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` es una lista de cero o más argumentos, cada uno de ellos puede ser de cadena o entero.

-   Cadena = miembro del objeto de acceso por clave.
-   Entero positivo = acceder al n-ésimo miembro / clave desde el principio.
-   Entero negativo = acceder al n-ésimo miembro / clave desde el final.

El índice mínimo del elemento es 1. Por lo tanto, el elemento 0 no existe.

Puede usar enteros para acceder a matrices JSON y objetos JSON.

Entonces, por ejemplo:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices\_or\_keys\]…) {#jsonlengthjson-indices-or-keys}

Devuelve la longitud de una matriz JSON o un objeto JSON.

Si el valor no existe o tiene un tipo incorrecto, `0` serán devueltos.

Ejemplos:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices\_or\_keys\]…) {#jsontypejson-indices-or-keys}

Devuelve el tipo de un valor JSON.

Si el valor no existe, `Null` serán devueltos.

Ejemplos:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices\_or\_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices\_or\_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices\_or\_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices\_or\_keys\]…) {#jsonextractbooljson-indices-or-keys}

Analiza un JSON y extrae un valor. Estas funciones son similares a `visitParam` función.

Si el valor no existe o tiene un tipo incorrecto, `0` serán devueltos.

Ejemplos:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices\_or\_keys\]…) {#jsonextractstringjson-indices-or-keys}

Analiza un JSON y extrae una cadena. Esta función es similar a `visitParamExtractString` función.

Si el valor no existe o tiene un tipo incorrecto, se devolverá una cadena vacía.

El valor es sin escape. Si no se pudo desescapar, devuelve una cadena vacía.

Ejemplos:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices\_or\_keys…\], Return\_type) {#jsonextractjson-indices-or-keys-return-type}

Analiza un JSON y extrae un valor del tipo de datos ClickHouse dado.

Esta es una generalización de la anterior `JSONExtract<type>` función.
Esto significa
`JSONExtract(..., 'String')` devuelve exactamente lo mismo que `JSONExtractString()`,
`JSONExtract(..., 'Float64')` devuelve exactamente lo mismo que `JSONExtractFloat()`.

Ejemplos:

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

Analiza los pares clave-valor de un JSON donde los valores son del tipo de datos ClickHouse especificado.

Ejemplo:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)]
```

## JSONExtractRaw(json\[, indices\_or\_keys\]…) {#jsonextractrawjson-indices-or-keys}

Devuelve una parte de JSON como cadena sin analizar.

Si la pieza no existe o tiene un tipo incorrecto, se devolverá una cadena vacía.

Ejemplo:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices\_or\_keys…\]) {#jsonextractarrayrawjson-indices-or-keys}

Devuelve una matriz con elementos de matriz JSON, cada uno representado como cadena sin analizar.

Si la parte no existe o no es una matriz, se devolverá una matriz vacía.

Ejemplo:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

## JSONExtractKeysAndValuesRaw {#json-extract-keys-and-values-raw}

Extrae datos sin procesar de un objeto JSON.

**Sintaxis**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**Parámetros**

-   `json` — [Cadena](../data-types/string.md) con JSON válido.
-   `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [cadena](../data-types/string.md) para obtener el campo por la clave o un [entero](../data-types/int-uint.md) para obtener el campo N-ésimo (indexado desde 1, los enteros negativos cuentan desde el final). Si no se establece, todo el JSON se analiza como el objeto de nivel superior. Parámetro opcional.

**Valores devueltos**

-   Matriz con `('key', 'value')` tuplas. Ambos miembros de tupla son cadenas.
-   Vacíe la matriz si el objeto solicitado no existe o si la entrada JSON no es válida.

Tipo: [Matriz](../data-types/array.md)([Tupla](../data-types/tuple.md)([Cadena](../data-types/string.md), [Cadena](../data-types/string.md)).

**Ejemplos**

Consulta:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')
```

Resultado:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Consulta:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')
```

Resultado:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Consulta:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')
```

Resultado:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
