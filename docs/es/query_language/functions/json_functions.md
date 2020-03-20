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

## ¿Cómo puedo hacerlo?\]…) {#jsonhasjson-indices-or-keys}

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

## ¿Cómo puedo hacerlo?\]…) {#jsonlengthjson-indices-or-keys}

Devuelve la longitud de una matriz JSON o un objeto JSON.

Si el valor no existe o tiene un tipo incorrecto, `0` serán devueltos.

Ejemplos:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## ¿Cómo puedo hacerlo?\]…) {#jsontypejson-indices-or-keys}

Devuelve el tipo de un valor JSON.

Si el valor no existe, `Null` serán devueltos.

Ejemplos:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## ¿Cómo puedo hacerlo?\]…) {#jsonextractuintjson-indices-or-keys}

## ¿Cómo puedo hacerlo?\]…) {#jsonextractintjson-indices-or-keys}

## ¿Cómo puedo hacerlo?\]…) {#jsonextractfloatjson-indices-or-keys}

## ¿Cómo puedo hacerlo?\]…) {#jsonextractbooljson-indices-or-keys}

Analiza un JSON y extrae un valor. Estas funciones son similares a `visitParam` función.

Si el valor no existe o tiene un tipo incorrecto, `0` serán devueltos.

Ejemplos:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## Por ejemplo, puede utilizar el siguiente ejemplo:\]…) {#jsonextractstringjson-indices-or-keys}

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

## Por ejemplo, se puede utilizar el siguiente método:) {#jsonextractjson-indices-or-keys-return-type}

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

## JSONExtractKeysAndValues(json\[, indices\_or\_keys…\], value\_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

Analizar pares clave-valor de un JSON donde los valores son del tipo de datos ClickHouse dado.

Ejemplo:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)];
```

## ¿Cómo puedo hacerlo?\]…) {#jsonextractrawjson-indices-or-keys}

Devuelve una parte de JSON.

Si la pieza no existe o tiene un tipo incorrecto, se devolverá una cadena vacía.

Ejemplo:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## Por ejemplo, puede utilizar el siguiente ejemplo:\]…) {#jsonextractarrayrawjson-indices-or-keys}

Devuelve una matriz con elementos de matriz JSON, cada uno representado como cadena sin analizar.

Si la parte no existe o no es una matriz, se devolverá una matriz vacía.

Ejemplo:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/json_functions/) <!--hide-->
