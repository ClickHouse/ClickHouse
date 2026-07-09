---
description: 'Encuentre rápidamente términos de búsqueda en texto.'
keywords: ['búsqueda de texto completo', 'índice de texto', 'índice', 'índices']
sidebar_label: 'Búsqueda de texto completo con índices de texto'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'Búsqueda de texto completo con índices de texto'
doc_type: 'reference'
---

Los índices de texto (también conocidos como [índices invertidos](https://en.wikipedia.org/wiki/Inverted_index)) permiten realizar búsquedas de texto completo rápidas en datos textuales.
Un índice de texto almacena una asignación de tokens a los números de fila que contienen cada token.
Los tokens se generan mediante un proceso llamado tokenización.
Por ejemplo, el tokenizador predeterminado de ClickHouse convierte la oración en inglés &quot;The cat likes mice.&quot; en los tokens [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;].

Por ejemplo, supongamos una tabla con una sola columna y tres filas

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

Los tokens correspondientes son:

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

Solemos buscar sin distinguir entre mayúsculas y minúsculas, por lo que pasamos los tokens a minúsculas:

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

También eliminaremos palabras vacías como &quot;I&quot;, &quot;the&quot; y &quot;and&quot;, ya que aparecen en casi todas las filas:

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

Un índice de texto contiene, conceptualmente, la siguiente información:

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

Dado un token de búsqueda, esta estructura de índice permite encontrar rápidamente todas las filas que coinciden.

<div id="creating-a-text-index">
  ## Crear un índice de texto
</div>

Los índices de texto están disponibles de forma general (GA) a partir de la versión 26.2 de ClickHouse.
En estas versiones, no es necesario configurar ninguna opción especial para usar el índice de texto.
Recomendamos encarecidamente usar versiones de ClickHouse &gt;= 26.2 en entornos de producción.

:::note
Los índices de texto pueden usarse con cualquier versión de ClickHouse &gt;= 26.2, independientemente de la configuración de [compatibilidad](../../../operations/settings/settings#compatibility).
:::

Para crear un índice de texto, use la siguiente sintaxis:

```sql title="Query"
CREATE TABLE table
(
    key UInt64,
    str String,
    INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )
)
ENGINE = MergeTree
ORDER BY key
```

Los índices de texto se pueden definir en columnas de estos tipos:

* [String](/es/sql-reference/data-types/string.md) y [FixedString](/es/sql-reference/data-types/fixedstring.md),
* [Array(String)](/es/sql-reference/data-types/array.md) y [Array(FixedString)](/es/sql-reference/data-types/array.md),
* [Map](/es/sql-reference/data-types/map.md) (mediante las funciones [mapKeys](/es/sql-reference/functions/tuple-map-functions.md/#mapKeys) y [mapValues](/es/sql-reference/functions/tuple-map-functions.md/#mapValues)), y
* [JSON](/es/sql-reference/data-types/newjson.md) (mediante las funciones [JSONAllPaths](/es/sql-reference/functions/json-functions.md/#JSONAllPaths) y [`JSONAllValues`](/es/sql-reference/functions/json-functions.md#JSONAllValues)).

También se admiten columnas de tipo [Nullable(T)](/es/sql-reference/data-types/nullable.md) y [LowCardinality()](/es/sql-reference/data-types/lowcardinality.md), incluido el tipo `Array(Nullable(String or FixedString))`.

Como alternativa, para agregar un índice de texto a una tabla existente:

```sql title="Query"
ALTER TABLE table
    ADD INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )

```

Si añade un índice a una tabla existente, le recomendamos materializarlo en las partes existentes de la tabla (de lo contrario, las búsquedas sobre las partes sin índice recurrirán a búsqueda por fuerza bruta lentos).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

Para eliminar un índice de texto, ejecute

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**Argumento `tokenizer` (obligatorio)**. El argumento `tokenizer` especifica el tokenizador:

* `splitByNonAlpha` divide cadenas por caracteres ASCII no alfanuméricos (consulte la función [splitByNonAlpha](/es/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
* `splitByString(S)` divide cadenas usando determinadas cadenas separadoras `S` definidas por el usuario (consulte la función [splitByString](/es/sql-reference/functions/splitting-merging-functions.md/#splitByString)).
  Los separadores pueden especificarse mediante un parámetro opcional; por ejemplo, `tokenizer = splitByString([', ', '; ', '\n', '\\'])`.
  Tenga en cuenta que cada cadena puede constar de varios caracteres (`', '` en el ejemplo).
  La lista de separadores predeterminada, si no se especifica explícitamente (por ejemplo, `tokenizer = splitByString`), es un único espacio en blanco `[' ']`.
* `asciiCJK` divide cadenas en tokens siguiendo las reglas de límites de palabras de Unicode (similar a [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)). Los caracteres ASCII alfanuméricos y los guiones bajos forman tokens con conectores (ASCII `:` para letras, `.` y `'` para caracteres del mismo tipo). Los caracteres Unicode no ASCII, incluidos los caracteres [CJK](https://en.wikipedia.org/wiki/CJK_characters), se convierten en tokens de un solo carácter.
* `ngrams(N)` divide cadenas en n-grams de tamaño uniforme `N` (consulte la función [ngrams](/es/sql-reference/functions/splitting-merging-functions.md/#ngrams)).
  La longitud del ngram puede especificarse mediante un parámetro entero opcional entre 1 y 8; por ejemplo, `tokenizer = ngrams(3)`.
  El tamaño predeterminado del ngram, si no se especifica explícitamente (por ejemplo, `tokenizer = ngrams`), es 3.
* `sparseGrams(min_length, max_length, min_cutoff_length)` divide cadenas en n-grams de longitud variable de al menos `min_length` y como máximo `max_length` caracteres (inclusive) (consulte la función [sparseGrams](/es/sql-reference/functions/string-functions#sparseGrams)).
  A menos que se especifique explícitamente, `min_length` y `max_length` toman los valores predeterminados 3 y 100.
  Si se proporciona el parámetro `min_cutoff_length`, solo se devuelven n-grams con una longitud mayor o igual que `min_cutoff_length`.
  En comparación con `ngrams(N)`, el tokenizador `sparseGrams` produce N-grams de longitud variable, lo que permite una representación más flexible del texto original.
  Por ejemplo, `tokenizer = sparseGrams(3, 5, 4)` genera internamente 3-, 4- y 5-grams a partir de la cadena de entrada, pero solo se devuelven los 4- y 5-grams.
* `array` no realiza tokenización; es decir, el valor de cada fila es un token (consulte la función [array](/es/sql-reference/functions/array-functions.md/#array)).

Todos los tokenizadores disponibles se enumeran en [system.tokenizers](../../../operations/system-tables/tokenizers.md).

:::note
El tokenizador `splitByString` aplica los separadores de división de izquierda a derecha.
Esto puede generar ambigüedades.
Por ejemplo, las cadenas separadoras `['%21', '%']` harán que `%21abc` se tokenice como `['abc']`, mientras que, si se invierte el orden de ambas cadenas separadoras a `['%', '%21']`, la salida será `['21abc']`.
En la mayoría de los casos, conviene que la coincidencia dé prioridad a los separadores más largos.
Por lo general, esto puede lograrse pasando las cadenas separadoras en orden descendente de longitud.
Si las cadenas separadoras forman un [código prefijo](https://en.wikipedia.org/wiki/Prefix_code), pueden pasarse en cualquier orden.
:::

Para entender cómo un tokenizador divide la cadena de entrada, puede usar las funciones [tokens](/es/sql-reference/functions/splitting-merging-functions.md/#tokens) y [tokensForLikePattern](/es/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern):

Ejemplo:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*Uso de entradas no ASCII.*
Los índices de texto pueden construirse a partir de datos de texto en cualquier idioma y conjunto de caracteres.
Para texto no ASCII, se recomienda el tokenizador `asciiCJK`, ya que maneja correctamente los límites de palabras de Unicode, incluidos los caracteres CJK.
:::

**Argumento del preprocesador (opcional)**. El preprocesador es una expresión que se aplica a la cadena de entrada antes de la tokenización.

Entre los casos de uso habituales del argumento del preprocesador se incluyen

1. Conversión a minúsculas/mayúsculas, o case folding para permitir la coincidencia sin distinción entre mayúsculas y minúsculas; p. ej., [lower](/es/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/es/sql-reference/functions/string-functions.md/#lowerUTF8), [caseFoldUTF8](/es/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. Normalización UTF-8; p. ej., [normalizeUTF8NFC](/es/sql-reference/functions/string-functions.md/#normalizeUTF8NFC), [normalizeUTF8NFD](/es/sql-reference/functions/string-functions.md/#normalizeUTF8NFD), [normalizeUTF8NFKC](/es/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC), [normalizeUTF8NFKD](/es/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD), [normalizeUTF8NFKCCasefold](/es/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold), [toValidUTF8](/es/sql-reference/functions/string-functions.md/#toValidUTF8).
3. Eliminación o transformación de caracteres o subcadenas no deseados, como los acentos; p. ej., [extractTextFromHTML](/es/sql-reference/functions/string-functions.md/#extractTextFromHTML), [substring](/es/sql-reference/functions/string-functions.md/#substring), [idnaEncode](/es/sql-reference/functions/string-functions.md/#idnaEncode), [translate](/es/sql-reference/functions/string-replace-functions.md/#translate), [removeDiacriticsUTF8](/es/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

La expresión del preprocesador debe transformar un valor de entrada de tipo [String](/es/sql-reference/data-types/string.md) o [FixedString](/es/sql-reference/data-types/fixedstring.md) en un valor del mismo tipo.
Si el índice de texto se creó sobre una columna de tipo `Nullable(T)` o `LowCardinality(T)`, la expresión del preprocesador debe aceptar valores anulables o de baja cardinalidad (es decir, no debe lanzar una excepción).

Ejemplos:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

Además, la expresión del preprocesador solo debe hacer referencia a la columna o expresión sobre la que se define el índice de texto.

Ejemplos:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* No permitido: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

No se permite el uso de funciones no deterministas.

:::note
En principio, los preprocesadores equivalen a envolver la columna o expresión del índice con la expresión del preprocesador.
Por ejemplo, el preprocesador `lower` en `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` puede emularse con `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`.
La desventaja de esta última forma es que el preprocesador emulado solo se aplica si coincide con la condición de filtro de la cláusula WHERE.
Por ejemplo, `WHERE hasAllTokens(lower(col), [...])` coincide, mientras que `WHERE hasAllTokens(col, [...])` no.
Por lo tanto, para una experiencia de usuario óptima, recomendamos usar expresiones de preprocesador.
:::

Las funciones [hasToken](/es/sql-reference/functions/string-search-functions.md/#hasToken), [hasAllTokens](/es/sql-reference/functions/string-search-functions.md/#hasAllTokens), [hasAnyTokens](/es/sql-reference/functions/string-search-functions.md/#hasAnyTokens) y [hasPhrase](/es/sql-reference/functions/string-search-functions.md/#hasPhrase) usan el preprocesador para transformar primero el término de búsqueda antes de tokenizarlo.
Tenga en cuenta que, como el preprocesador solo se aplica en la ruta del índice de texto, los resultados de estas funciones pueden diferir entre las consultas que usan el índice de texto y las que no lo usan (p. ej., `SETTINGS use_skip_indexes = 0`).

Por ejemplo,

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, 'Foo');
```

equivale a:

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx lower(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, lower('Foo'));
```

En este caso, la expresión del preprocesador transforma cada uno de los elementos del array.

Ejemplo:

```sql title="Query"
CREATE TABLE table
(
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr))

    -- This is not legal:
    INDEX idx_illegal arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arraySort(arr))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(arr, 'foo');
```

Para definir un preprocesador en un índice de texto en columnas de tipo [Map](/es/sql-reference/data-types/map.md), los usuarios deben decidir si el índice está
construido sobre las claves o los valores del mapa.

Ejemplo:

```sql title="Query"
CREATE TABLE table
(
    map Map(String, String),
    INDEX idx mapKeys(map)  TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(map)))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'foo');
```

**Argumento de posprocesamiento (opcional)**. El posprocesamiento hace referencia a una expresión que se aplica a cada token de salida después de la tokenización.

A diferencia del preprocesador, que transforma toda la cadena de entrada antes de que el tokenizador la divida en tokens, el posprocesamiento actúa sobre los propios tokens, uno por uno.
Este es el lugar natural para transformaciones que son inherentemente a nivel de token.

Los casos de uso típicos del argumento de posprocesamiento incluyen:

1. **Filtrado de palabra vacía (tokens extremadamente frecuentes)**. Los tokens muy comunes, como &quot;the&quot;, &quot;a&quot; e &quot;is&quot;, aportan poca relevancia a la búsqueda e inflan el índice.
   Puedes usar el posprocesamiento para descartarlos convirtiéndolos en tokens vacíos; los tokens vacíos se ignoran, es decir, no se añaden al índice.
   Ejemplo: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **Eliminación de timestamp**. Las líneas de log suelen comenzar con un timestamp estructurado como `2024-01-15T10:23:45` o contenerlo.
   El indexing de tokens de timestamp infla el índice con cadenas que no aportan relevancia a la búsqueda.
   Hay dos enfoques complementarios para ignorar los timestamps:
   * **Enfoque de posprocesamiento**: usa el tokenizador `splitByString` (división por espacios en blanco) para que todo el timestamp se convierta en un único token, y luego usa `parseDateTimeOrNull` para detectarlo y descartarlo.
     Ejemplo: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     Para timestamps con desplazamientos de timezone o segundos fraccionarios, usa `parseDateTimeBestEffortOrNull(str)` sin una format string explícita.
   * **Enfoque de preprocesamiento**: elimina el timestamp de la línea de log completa *antes* de la tokenización mediante una regular expression.
     Ejemplo: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     Esto funciona con cualquier tokenizador y es más eficiente, ya que los caracteres del timestamp nunca llegan a tokenizarse.
     Ambos enfoques pueden combinarse: el preprocesador elimina el timestamp mientras que el posprocesamiento normaliza o filtra los tokens restantes (por ejemplo, convertir a minúsculas + descartar palabras de severidad como `ERROR` o `INFO`).
3. **Stemming**. Convertir cada token en su raíz mejora la exhaustividad de la búsqueda al hacer coincidir variantes morfológicas que comparten la misma raíz.
   Por ejemplo, con stemming en inglés, &quot;running&quot;, &quot;runs&quot; y &quot;run&quot; se reducen todos a &quot;run&quot;, por lo que una consulta de cualquiera de estas variantes coincide con todas ellas.
   ClickHouse proporciona una función [stem](/es/sql-reference/functions/string-functions.md/#stem) integrada para varios idiomas.
   Ejemplo: `stem(str, 'en')`
4. **Normalización de mayúsculas y minúsculas**. Convertir los tokens a minúsculas o mayúsculas para permitir una correspondencia case-insensitive; por ejemplo, [lower](/es/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/es/sql-reference/functions/string-functions.md/#lowerUTF8).
   Para convertir a minúsculas o mayúsculas, recomendamos usar un preprocesador en lugar de un posprocesamiento.

La expresión de posprocesamiento transforma tokens de tipo [String](/es/sql-reference/data-types/string.md) en tokens del mismo tipo.
Además, la expresión de posprocesamiento solo debe hacer referencia a la columna o expresión sobre la que se define el text index.
Cuando la columna es de tipo `Array(String)`, el posprocesamiento sigue actuando sobre tokens individuales como valores `String` simples.

No se permite el uso de funciones no deterministas.

El posprocesamiento se aplica a cada token generado durante la creación del índice (para el tokenizer `array`, cada elemento del array es un token). Al ejecutar la consulta, el comportamiento depende de la función:

* Para `hasToken`, `hasAllTokens`, `hasAnyTokens` y `hasPhrase` (con cualquier tokenizer compatible): el posprocesamiento se aplica tanto a los tokens del texto analizado como al término de búsqueda, lo que permite una coincidencia totalmente normalizada (p. ej., búsqueda sin distinguir entre mayúsculas y minúsculas). En `hasPhrase`, los tokens posprocesados se colocan de forma compacta, de modo que si el posprocesamiento descarta un token, no queda ningún hueco posicional y la frase sigue coincidiendo a través de él; por ejemplo, con un posprocesamiento de palabras vacías que elimina `the`, `hasPhrase(col, 'see cat')` coincide con un documento `see the cat`.
* Para todas las demás funciones (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`): solo se aplica posprocesamiento al término de búsqueda para la búsqueda asistida por índice; el predicado a nivel de fila sigue comparándose con los valores originales de la columna.

Ejemplos:

* Eliminar palabras vacías usando una expresión de posprocesamiento:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)
    )
)
ENGINE = MergeTree
ORDER BY tuple();
```

* Elimine las marcas temporales mediante una expresión de posprocesamiento:

```sql
-- Log lines: '2024-01-15T10:23:45 ERROR connection failed'
-- The splitByString tokenizer (default: whitespace) keeps the full timestamp as one token.
-- parseDateTimeOrNull detects and drops it; non-timestamp words are kept.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByString',
        postprocessor = if(isNull(parseDateTimeOrNull(line, '%Y-%m-%dT%H:%i:%S')), line, '')
    )
)
ENGINE = MergeTree ORDER BY id;

-- Only message-level words are indexed; timestamp tokens are not stored.
SELECT count() FROM logs WHERE hasAllTokens(line, ['ERROR']);       -- fast index lookup
SELECT count() FROM logs WHERE hasAllTokens(line, ['2024-01-15T10:23:45']);  -- returns 0: token was never indexed
```

* Elimine las marcas de tiempo con una expresión de preprocesamiento:

```sql
-- The preprocessor strips the ISO timestamp prefix before tokenization.
-- Any tokenizer can be used; timestamp characters are never seen by the tokenizer.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer   = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')
    )
)
ENGINE = MergeTree ORDER BY id;
```

* Elimina las marcas de tiempo con una expresión combinada de preprocesamiento y posprocesamiento:

```sql
-- Preprocessor strips the timestamp, then lowercases the remainder.
-- Postprocessor drops the severity word (error, info, warn, debug) after tokenization.
-- Result: only substantive message words are stored in the index.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByNonAlpha',
        preprocessor = lower(replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')),
        postprocessor = if(line IN ('error', 'info', 'warn', 'warning', 'debug', 'critical'), '', line)
    )
)
ENGINE = MergeTree ORDER BY id;

-- Example log line: '2024-01-15T10:23:45 ERROR connection failed'
-- After preprocessor:  'error connection failed'
-- After tokenization:  ['error', 'connection', 'failed']
-- After postprocessor: ['connection', 'failed']   ← 'error' dropped as severity word
SELECT count() FROM logs WHERE hasAllTokens(line, ['connection']);
```

* Reduzca los tokens a su raíz mediante una expresión de posprocesamiento:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = stem(str, 'en')
    )
)
ENGINE = MergeTree
ORDER BY tuple();

-- The query token 'running' is stemmed to 'run' before the lookup,
-- matching rows that contain 'run', 'runs', 'ran', 'running', etc.
SELECT count() FROM table WHERE hasAllTokens(str, ['running']);
```

**Compatibilidad de funciones**.

Para los predicados que consultan el índice de texto, el preprocesador y el posprocesamiento se aplican al valor de búsqueda antes de la comprobación a nivel de gránulo, de modo que la búsqueda en el índice use los mismos tokens que se almacenaron al crear el índice.
Para la mayoría de las funciones (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`), el índice de texto se usa solo para omitir bloques de datos irrelevantes; ClickHouse sigue verificando cada fila resultante con el predicado original sobre los datos originales de la columna.
Para las funciones de búsqueda de tokens (`hasToken`, `hasAllTokens`, `hasAnyTokens`), el índice de texto es la vía principal de evaluación: ClickHouse normaliza la cadena buscada mediante el mismo preprocesador, tokenizador y posprocesamiento que se aplicaron al crear el índice, y usa esta forma normalizada tanto para las partes de la tabla indexadas como para las no indexadas. Con un posprocesamiento, los tokens del texto buscado también se normalizan en el momento de la consulta (para cualquier tokenizador, no solo `array`), de modo que ambos lados de la comparación se transforman de forma coherente y el resultado no depende de si el índice se lee directamente (ajuste `query_plan_direct_read_from_text_index`) ni de si una parte determinada tiene un índice materializado; por ejemplo, al habilitar la coincidencia sin distinción entre mayúsculas y minúsculas para `hasAllTokens(col, ['FOO'])` con un posprocesamiento `lower`.
Sin `positions`, `hasPhrase` usa el índice solo como pista y verifica cada fila resultante con el predicado original; además, un posprocesamiento normaliza tanto la frase como los tokens del texto buscado de la misma manera, por lo que el resultado es independiente de la ruta de lectura, y los tokens que el posprocesamiento descarta no rompen la adyacencia de la frase. Con `positions = 1`, `hasPhrase` usa lecturas directas exactas (y sigue aplicando el posprocesamiento, si lo hay).
Los tokens de búsqueda que el posprocesamiento transforma en una cadena vacía se ignoran; es decir, se tratan como ausentes de la frase de búsqueda.

| Función                                                                                     | Admite un preprocesador                          | Tokenizadores compatibles                                | Admite posprocesamiento |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------- | ----------------------- |
| `=`                                                                                         | sí                                               | todos                                                    | sí                      |
| `IN`                                                                                        | sí                                               | todos                                                    | sí                      |
| [hasToken](/es/sql-reference/functions/string-search-functions.md/#hasToken)                   | sí                                               | todos (diseñado para `splitByNonAlpha`)                  | sí                      |
| [hasAnyTokens(col, str)](/es/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | sí                                               | todos                                                    | sí                      |
| [hasAllTokens(col, str)](/es/sql-reference/functions/string-search-functions.md/#hasAllTokens) | sí                                               | todos                                                    | sí                      |
| [hasAnyTokens(col, arr)](/es/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | no (los elementos del array son tokens tal cual) | todos                                                    | sí                      |
| [hasAllTokens(col, arr)](/es/sql-reference/functions/string-search-functions.md/#hasAllTokens) | no (los elementos del array son tokens tal cual) | todos                                                    | sí                      |
| [hasPhrase](/es/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | sí                                               | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | sí                      |
| [startsWith](/es/sql-reference/functions/string-functions.md/#startsWith)                      | sí                                               | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sí                      |
| [endsWith](/es/sql-reference/functions/string-functions.md/#endsWith)                          | sí                                               | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sí                      |
| [like](/es/sql-reference/functions/string-search-functions.md/#like)                           | sí¹                                              | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | sí¹                     |
| [match](/es/sql-reference/functions/string-search-functions.md/#match)                         | sí¹                                              | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | sí¹                     |
| [ilike](/es/sql-reference/functions/string-search-functions.md/#like)                          | sí² (`lower`/`upper` únicamente)                 | `splitByNonAlpha`, `array`²                              | no²                     |
| [mapContainsKey](/es/sql-reference/functions/tuple-map-functions#mapContainsKey)               | sí                                               | todos                                                    | sí                      |
| [mapContainsValue](/es/sql-reference/functions/tuple-map-functions#mapContainsValue)           | sí                                               | todos                                                    | sí                      |
| [mapContainsKeyLike](/es/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | sí                                               | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sí                      |
| [mapContainsValueLike](/es/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | sí                                               | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sí                      |
| [has](/es/sql-reference/functions/array-functions.md/#has)                                     | sí                                               | `array`                                                  | sí                      |
| [hasAny](/es/sql-reference/functions/array-functions.md/#hasAny)                               | sí                                               | `array`                                                  | sí                      |
| [hasAll](/es/sql-reference/functions/array-functions.md/#hasAll)                               | sí                                               | `array`                                                  | sí                      |

¹ `LIKE` y `match` usan lectura directa como pista para los tokenizadores indicados; de lo contrario, recurren a un barrido por fuerza bruta.
Además, `LIKE` admite *lectura directa (without hint)* (se habilita mediante `use_text_index_like_evaluation_by_dictionary_scan`) para los tokenizadores `splitByNonAlpha` y `array` sin preprocesador ni posprocesamiento.

² `ILIKE` solo es compatible mediante lectura directa (without hint) (`use_text_index_like_evaluation_by_dictionary_scan = 1`, tokenizador `splitByNonAlpha` o `array`).
No existe un mecanismo de fallback que use el índice como pista: si la configuración está deshabilitada o el tokenizador no está en el conjunto compatible, el índice no se usa para `ILIKE`.
El preprocesador, si está presente, debe ser `lower` o `upper`; no se admiten posprocesamientos.

**Experimental: argumento Positions (opcional)**.

El parámetro experimental `positions` (valor predeterminado: `0`) controla si el índice almacena posiciones de tokens.
Cuando se establece en `1`, el índice almacena además datos posicionales (en un archivo `.pos`), lo que permite la coincidencia exacta de frases mediante lectura directa para la función [`hasPhrase`](#functions-example-hasphrase).
Almacenar posiciones aumenta el tamaño en disco del índice y el costo de escritura, por lo que es una función de activación opcional.
El formato en disco aún no es estable, por lo que este parámetro es experimental y puede cambiar en una versión futura.
Por lo tanto, crear un índice con `positions = 1` requiere que la configuración de MergeTree [`allow_experimental_text_index_positions`](/es/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) esté habilitada.
Establezca `positions = 0` (el valor predeterminado) para conservar el almacenamiento basado únicamente en posting lists; los índices de texto creados sin este argumento siguen sin posiciones.

:::warning
Este argumento es experimental y solo debe usarse para pruebas.
Establezca la configuración de MergeTree [`allow_experimental_text_index_positions`](/es/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) para habilitar el almacenamiento de posiciones.
:::

<details markdown="1">
  <summary>Parámetros avanzados opcionales</summary>

  Los valores predeterminados de los siguientes parámetros avanzados funcionarán bien en prácticamente todas las situaciones.
  No recomendamos cambiarlos.

  El parámetro opcional `dictionary_block_size` (valor predeterminado: 512) especifica el tamaño de los bloques del diccionario en filas.

  El parámetro opcional `dictionary_block_frontcoding_compression` (valor predeterminado: 1) especifica si los bloques del diccionario usan front-coding como compresión.

  El parámetro opcional `posting_list_block_size` (valor predeterminado: 1048576) especifica el tamaño de los bloques de posting lists en filas.

  El parámetro opcional `posting_list_codec` (valor predeterminado: `none`) especifica el códec de la posting list:

  * `none` - las posting lists se almacenan sin compresión adicional.
  * `bitpacking` - aplica [codificación diferencial (delta)](https://en.wikipedia.org/wiki/Delta_encoding), seguida de [bit-packing](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (cada uno dentro de bloques de tamaño fijo). Ralentiza las consultas SELECT; por ahora no se recomienda.

  Como alternativa, los parámetros avanzados anteriores pueden configurarse a nivel de tabla mediante las configuraciones de MergeTree correspondientes: [`text_index_dictionary_block_size`](/es/operations/settings/merge-tree-settings#text_index_dictionary_block_size), [`text_index_dictionary_block_frontcoding_compression`](/es/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression), [`text_index_posting_list_block_size`](/es/operations/settings/merge-tree-settings#text_index_posting_list_block_size) y [`text_index_posting_list_codec`](/es/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  Se aplican a cada índice de texto de la tabla que no especifique explícitamente el parámetro.

  El principal caso de uso de las configuraciones a nivel de tabla es cambiar los parámetros del índice de una tabla existente sin eliminar ni volver a crear el índice de texto en todas las partes de la tabla.
  Cambiar una configuración a nivel de tabla aplica los nuevos parámetros solo a los índices de texto creados para partes nuevas; las partes existentes conservan su estructura actual.

  Un argumento proporcionado en la definición del índice tiene prioridad sobre la configuración de la tabla; por ejemplo:

  ```sql
  CREATE TABLE table(
      s String,
      -- Este índice usa 'bitpacking' y sobrescribe el valor predeterminado a nivel de tabla de abajo:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- Este índice hereda 'none' de la configuración de la tabla:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*Granularidad del índice.*
Los índices de texto se implementan en ClickHouse como un tipo de [skip indexes](/es/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types).
Sin embargo, a diferencia de otros skip indexes, los índices de texto usan una granularidad infinita (100 millones).
Esto puede verse en la definición de tabla de un índice de texto.

Ejemplo:

```sql title="Query"
CREATE TABLE table(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(2)))
ENGINE = MergeTree()
ORDER BY k;

SHOW CREATE TABLE table;
```

```result title="Response"
┌─statement──────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.table                                            ↴│
│↳(                                                                     ↴│
│↳    `k` UInt64,                                                       ↴│
│↳    `s` String,                                                       ↴│
│↳    INDEX idx s TYPE text(tokenizer = ngrams(2)) GRANULARITY 100000000↴│ <-- here
│↳)                                                                     ↴│
│↳ENGINE = MergeTree                                                    ↴│
│↳ORDER BY k                                                            ↴│
│↳SETTINGS index_granularity = 8192                                      │
└────────────────────────────────────────────────────────────────────────┘
```

La elevada granularidad del índice garantiza que el índice de texto se cree para la parte completa.
Se ignora una granularidad de índice especificada explícitamente.

<div id="using-a-text-index">
  ## Uso de un índice de texto
</div>

Usar un índice de texto en consultas SELECT es sencillo, ya que las funciones habituales de búsqueda en cadenas utilizarán el índice automáticamente.
Si no existe ningún índice en una columna o en un fragmento de tabla, las funciones de búsqueda en cadenas recurrirán a búsquedas lentas por fuerza bruta.

:::note
Recomendamos usar las funciones `hasAnyTokens` y `hasAllTokens` para buscar en el índice de texto; consulte [más abajo](#functions-example-hasanytokens-hasalltokens).
Estas funciones funcionan con todos los tokenizadores disponibles y con todas las expresiones posibles de preprocesamiento y posprocesamiento.
Como las demás funciones compatibles existían históricamente antes que el índice de texto, en muchos casos tuvieron que conservar su comportamiento heredado (p. ej., sin compatibilidad con preprocesamiento ni posprocesamiento).
:::

<div id="functions-support">
  ### Funciones compatibles
</div>

El índice de texto puede utilizarse si se usan funciones de texto en la cláusula `WHERE` o en las cláusulas `PREWHERE`:

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/es/sql-reference/functions/comparison-functions.md/#equals)) coincide con todo el término de búsqueda proporcionado.

Ejemplo:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/es/sql-reference/functions/in-functions)) es similar a `equals`, pero coincide con cualquiera de los términos de búsqueda.

Ejemplo:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
`NOT IN` (`notIn`) no es compatible con el índice de texto.
:::

<div id="functions-example-like-match">
  #### `LIKE` y `match`
</div>

:::note
Actualmente, estas funciones usan el índice de texto para filtrar solo si el `tokenizador` del índice es `splitByNonAlpha`, `ngrams` o `sparseGrams`.
:::

:::note
`NOT LIKE` (`notLike`) no es compatible con el índice de texto.
:::

Para usar `LIKE` ([like](/es/sql-reference/functions/string-search-functions.md/#like)) y la función [match](/es/sql-reference/functions/string-search-functions.md/#match) con índices de texto, ClickHouse debe poder extraer tokens completos del término de búsqueda.
En el caso del índice con `tokenizador` `ngrams`, esto sucede si la longitud de las cadenas buscadas entre comodines es igual o superior a la longitud del ngram.

Ejemplo para el índice de texto con `tokenizador` `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

`support` en el ejemplo podría coincidir con `support`, `supports`, `supporting`, etc.
Este tipo de consulta es una búsqueda por subcadenas y no puede acelerarse con un índice de texto.

Para aprovechar un índice de texto en consultas LIKE, el patrón de LIKE debe reescribirse de la siguiente manera:

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

Los espacios a la izquierda y a la derecha de `support` garantizan que el término pueda extraerse como un token.

Afortunadamente, hay un caso especial en el que ClickHouse puede aprovechar el índice invertido para acelerar considerablemente las consultas LIKE.

Consulta la sección de [optimización del rendimiento de LIKE/ILIKE](#like-ilike-queries-perf) para obtener más detalles.

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` y `multiMatchAny`
</div>

[multiSearchAny](/es/sql-reference/functions/string-search-functions.md/#multiSearchAny) y su variante UTF-8 [multiSearchAnyUTF8](/es/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) comprueban si alguna de varias subcadenas literales aparece en el texto de entrada, y [multiMatchAny](/es/sql-reference/functions/string-search-functions.md/#multiMatchAny) comprueba si alguna de varias expresiones regulares coincide.
Estas funciones usan el índice de texto en las mismas condiciones que `LIKE` y `match` (véase arriba): ClickHouse debe poder extraer tokens completos de cada patrón de búsqueda, y la lista de patrones debe ser constante.
Se lee un gránulo si alguno de los patrones puede estar presente en él.

En `multiMatchAny`, si un patrón no puede reducirse a un requisito de token (por ejemplo, `.*`, que coincide con cualquier documento), no se puede usar el índice de texto y la consulta pasa a un escaneo completo.

Al igual que con `LIKE` y `match`, la búsqueda de subcadenas y expresiones regulares funciona mejor con los tokenizers `ngrams` y `sparseGrams`.
Estos tokenizers indexan n-grams de caracteres superpuestos, por lo que un patrón de búsqueda se descompone en n-grams que están presentes en el índice allí donde el patrón aparece como una subcadena, independientemente de si empieza o termina en mitad de una palabra.
Por tanto, un patrón de búsqueda puede usarse tal cual, siempre que tenga al menos la misma longitud que el tamaño del n-gram.

Ejemplo del índice de texto con el tokenizer `ngrams`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

El tokenizador `splitByNonAlpha`, en cambio, solo indexa tokens completos (palabras enteras).
Como una subcadena de búsqueda puede empezar o terminar en medio de una palabra, ClickHouse descarta los tokens inicial y final de cada subcadena de búsqueda, por lo que el índice solo puede omitir gránulos usando tokens completos.
Para que la búsqueda de subcadenas y expresiones regulares use el índice con `splitByNonAlpha`, rodee cada subcadena de búsqueda con caracteres separadores (como espacios) para que forme uno o más tokens completos.

Ejemplo de índice de texto con el tokenizador `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` and `endsWith`
</div>

Al igual que `LIKE`, las funciones [startsWith](/es/sql-reference/functions/string-functions.md/#startsWith) y [endsWith](/es/sql-reference/functions/string-functions.md/#endsWith) solo pueden usar un índice de texto si pueden extraerse tokens completos del término de búsqueda.
En el caso del índice con el tokenizador `ngrams`, esto se cumple si la longitud de las cadenas buscadas entre comodín es igual o mayor que la longitud del ngram.
Cuando un índice de texto usa posprocesamiento, estas funciones aún pueden usar el índice en modo Hint si los hint tokens extraídos siguen sin quedar vacíos tras la normalización. Si la normalización elimina todos los hint tokens, el índice no se usa para ese predicado.

Ejemplo de índice de texto con el tokenizador `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

En el ejemplo, solo `clickhouse` se considera un token.
`support` no es un token porque puede coincidir con `support`, `supports`, `supporting`, etc.

Para encontrar todas las filas que empiezan por `clickhouse supports`, termine el patrón de búsqueda con un espacio al final:

```sql
startsWith(comment, 'clickhouse supports ')`
```

Del mismo modo, `endsWith` debe usarse con un espacio inicial:

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
`hasToken` tiene ciertas limitaciones cuando se usa para lookups en índices de texto con tokenizadores distintos de `splitByNonAlpha` y/o expresiones de preprocesamiento/posprocesamiento.
Recomendamos usar `hasAnyTokens` y `hasAllTokens` en su lugar.

Las variantes sin distinción entre mayúsculas y minúsculas `hasTokenCaseInsensitive` y `hasTokenCaseInsensitiveOrNull` no son compatibles con los índices de texto: siempre realizan un escaneo completo de filas, incluso en columnas indexadas con índices de texto. Para la coincidencia sin distinción entre mayúsculas y minúsculas, use un preprocesador o posprocesamiento `lower(...)` y combínelo con `hasToken` / `hasAllTokens` / `hasAnyTokens`.
:::

La función [hasToken](/es/sql-reference/functions/string-search-functions.md/#hasToken) busca coincidencias con un único token dado.

A diferencia de las funciones mencionadas anteriormente, estas no tokenizan el término de búsqueda (asumen que la entrada es un solo token).

Ejemplo:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` y `hasAllTokens`
</div>

Las funciones [hasAnyTokens](/es/sql-reference/functions/string-search-functions.md/#hasAnyTokens) y [hasAllTokens](/es/sql-reference/functions/string-search-functions.md/#hasAllTokens) buscan coincidencias con uno o con todos los tokens indicados.

Estas dos funciones aceptan los tokens de búsqueda como una cadena, que se tokenizará con el mismo tokenizador usado para la columna del índice, o como un array de tokens ya procesados, a los que no se les aplicará tokenización antes de la búsqueda.
Consulta la documentación de la función para obtener más información.

Ejemplo:

```sql
-- Search tokens passed as string argument
SELECT count() FROM table WHERE hasAnyTokens(comment, 'clickhouse olap');
SELECT count() FROM table WHERE hasAllTokens(comment, 'clickhouse olap');

-- Search tokens passed as Array(String)
SELECT count() FROM table WHERE hasAnyTokens(comment, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAllTokens(comment, ['clickhouse', 'olap']);
```

<div id="functions-example-hasphrase">
  #### `hasPhrase`
</div>

La función [hasPhrase](/es/sql-reference/functions/string-search-functions.md/#hasPhrase) busca una frase: todos los tokens deben aparecer de forma consecutiva y en el mismo orden que en la cadena de búsqueda.

A diferencia de `hasAllTokens`, que solo requiere que todos los tokens estén presentes en algún punto, `hasPhrase` exige que aparezcan como una secuencia consecutiva.
La frase de búsqueda se tokeniza con el mismo tokenizador configurado para la columna del índice.
Cuando el índice de texto usa un posprocesamiento, la frase de búsqueda también se normaliza antes de la búsqueda en el índice.
Ten en cuenta que la función requiere uno de estos tokenizadores: `splitByNonAlpha`, `splitByString`, `ngrams` o `asciiCJK`.

Ejemplo:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

La función `has` de Array [has](/es/sql-reference/functions/array-functions#has) encuentra coincidencias con un único token en el array de cadenas.

Ejemplo:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` y `hasAll`
</div>

Las funciones de Array [hasAny](/es/sql-reference/functions/array-functions#hasAny) y [hasAll](/es/sql-reference/functions/array-functions#hasAll) comprueban si la columna de Array indexada contiene alguna o todas las cadenas de búsqueda de un conjunto constante.

Ejemplo:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

La función [mapContains](/es/sql-reference/functions/tuple-map-functions#mapContainsKey) (un alias de `mapContainsKey`) busca coincidencias con los tokens extraídos de la cadena buscada en las claves de un mapa.
El comportamiento es similar al de la función `equals` con una columna `String`.
El índice de texto solo se utiliza si se creó sobre una expresión `mapKeys(map)`.

Ejemplo:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

La función [mapContainsValue](/es/sql-reference/functions/tuple-map-functions#mapContainsValue) busca coincidencias con los tokens extraídos de la cadena de búsqueda en los valores de un mapa.
El comportamiento es similar al de la función `equals` con una columna `String`.
El índice de texto solo se utiliza si se creó sobre una expresión `mapValues(map)`.

Ejemplo:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` and `mapContainsValueLike`
</div>

Las funciones [mapContainsKeyLike](/es/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) y [mapContainsValueLike](/es/sql-reference/functions/tuple-map-functions#mapContainsValueLike) comparan un patrón con todas las claves o valores (respectivamente) de un mapa.

Ejemplo:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

El [operador de acceso operator[]](/es/sql-reference/operators#access-operators) puede usarse con el índice de texto para filtrar claves y valores. El índice de texto solo se utiliza si se crea sobre las expresiones `mapKeys(map)` o `mapValues(map)`, o sobre ambas.

Ejemplo:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

Consulte los siguientes ejemplos para usar columnas de tipo `Array(T)` y `Map(K, V)` con el índice de texto.

<div id="text-index-example-array">
  ### Indexación de columnas Array(String)
</div>

Imagina una plataforma de blogs en la que los autores clasifican las entradas de su blog con palabras clave.
Queremos que los usuarios descubran contenido relacionado buscándolo o haciendo clic en los temas.

Considera esta definición de tabla:

```sql
CREATE TABLE posts
(
    post_id UInt64,
    title String,
    content String,
    keywords Array(String)
)
ENGINE = MergeTree
ORDER BY (post_id);
```

Sin un índice de texto, encontrar publicaciones con una palabra clave específica (p. ej., `clickhouse`) requiere recorrer todos los registros:

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

A medida que la plataforma crece, esto se vuelve cada vez más lento porque la consulta debe examinar cada Array `keywords` de cada fila.
Para solucionar este problema de rendimiento, definimos un índice de texto para la columna `keywords`:

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### Indexación de columnas de tipo Map
</div>

En muchos casos de uso de observabilidad, los mensajes de log se dividen en &quot;componentes&quot; y se almacenan con los tipos de datos adecuados; por ejemplo, fecha y hora para la marca temporal, enum para el nivel de registro, etc.
Los campos de métricas se almacenan mejor como pares clave-valor.
Los equipos de operaciones necesitan buscar de forma eficiente en los logs para tareas de depuración, incidentes de seguridad y monitorización.

Considere la siguiente tabla de logs:

```sql
CREATE TABLE logs
(
    id UInt64,
    timestamp DateTime,
    message String,
    attributes Map(String, String)
)
ENGINE = MergeTree
ORDER BY (timestamp);
```

Sin un índice de texto, buscar en datos de [Map](/es/sql-reference/data-types/map.md) requiere escaneos completos de la tabla:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

A medida que aumenta el volumen de logs, estas consultas se vuelven lentas.

La solución es crear un índice de texto para las claves y los valores de [Map](/es/sql-reference/data-types/map.md).
Use [mapKeys](/es/sql-reference/functions/tuple-map-functions.md/#mapKeys) para crear un índice de texto cuando necesite encontrar logs por nombres de campo o tipos de atributo:

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

Usa [mapValues](/es/sql-reference/functions/tuple-map-functions.md/#mapValues) para crear un índice de texto cuando necesites buscar en el contenido real de los atributos:

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

Consultas de ejemplo:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### Indexación de columnas JSON
</div>

Los índices de texto pueden usarse con columnas `JSON` de tres formas:

1. **Índices sobre subcolumnas específicas** — cree un índice de texto sobre una ruta JSON conocida, igual que sobre una columna normal. Esto indexa los *valores* de esa ruta.
2. **Índices basados en rutas con [JSONAllPaths](/es/sql-reference/functions/json-functions.md/#JSONAllPaths)** — indexan *todas las rutas* presentes en cada gránulo para omitir los gránulos que no pueden contener la ruta consultada. Es similar a las columnas `Map`.
3. **Índices basados en valores con [JSONAllValues](/es/sql-reference/functions/json-functions.md#JSONAllValues)** — indexan *todos los valores* de todas las rutas JSON para acelerar la búsqueda de texto completo en cualquier subcolumna JSON con un único índice.

<div id="json-indexes-on-subcolumns">
  #### Índices en subcolumnas específicas
</div>

Puede crear un índice de omisión en cualquier subcolumna de JSON usando la misma sintaxis que para las columnas normales.

Hay dos formas de hacer referencia a una subcolumna de JSON en una expresión de índice:

* **Ruta tipada** declarada en la indicación de tipo JSON: acceda directamente por nombre: `json.a`.
* **Ruta dinámica** con conversión explícita: use la sintaxis de conversión `::`: `json.b::String`.

Definición de ejemplo del índice:

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id String),
    INDEX idx_sensor data.sensor_id TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_location data.location::String TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number , 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

Consulta de ejemplo:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id = 'id_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: text
        Condition: (mode: All; tokens: ["5", "id"])
        Parts: 1/2
        Granules: 1/8
```

Ejemplo de consulta:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: text
        Condition: (mode: All; tokens: ["5", "room"])
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  #### Índices basados en rutas con JSONAllPaths
</div>

Al igual que con las columnas `Map`, se pueden crear índices de texto en columnas [JSON](/es/sql-reference/data-types/newjson.md) mediante [`JSONAllPaths`](/es/sql-reference/functions/json-functions.md/#JSONAllPaths).
El índice almacena el conjunto de rutas JSON presentes en cada gránulo y las utiliza para omitir gránulos en los que la ruta consultada no está presente.

Definición de ejemplo del índice:

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

Puedes usar `EXPLAIN indexes = 1` para verificar que se está usando el índice de omisión.
Cuando una ruta existe solo en una parte, el índice omite la otra parte.

Ejemplo:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

Si una ruta no existe en ninguna parte, se omiten todas las partes y los gránulos.

Ejemplo:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["nonexistent"])
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` también usa el índice: omite los gránulos en los que la ruta no está presente (ya que el valor sería `NULL`):

Ejemplo:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallvalues">
  #### Índices basados en valores con JSONAllValues
</div>

Los índices de texto pueden usarse para acelerar las búsquedas en columnas [JSON](/es/sql-reference/data-types/newjson.md) mediante la función [`JSONAllValues`](/es/sql-reference/functions/json-functions.md#JSONAllValues).

`JSONAllValues` devuelve todos los valores de una columna JSON como `Array(String)`.
Los valores de tipos de datos que no son cadenas (p. ej., enteros y arrays) se convierten a su representación textual.
Un índice de texto creado con `JSONAllValues` indexa estas representaciones textuales en todas las rutas JSON de cada fila.
Este índice puede acelerar después las consultas que filtran por subcolumnas JSON individuales.
Cuando una consulta filtra por una subcolumna específica (p. ej., `data.user_name = 'alice'`), el índice de texto puede omitir rápidamente las filas (y gránulos) que no contienen los tokens de búsqueda en ninguno de sus valores JSON.

:::note
El índice puede producir falsos positivos cuando distintas rutas JSON contienen los mismos tokens.
Por ejemplo, si la fila 1 tiene `{"a": "hello", "b": "world"}` y una consulta busca `data.a = 'world'`, el índice de texto no puede distinguir que `world` pertenece a la ruta `b`, no a `a`.
En esos casos, el índice no omitirá la fila, y el filtro sobre los datos reales de la columna se encargará de la evaluación final.
Este es el mismo comportamiento que en otros casos de uso de índices de texto, donde el índice actúa como un prefiltro rápido.
:::

<div id="json-all-values-creating-the-index">
  ##### Crear el índice
</div>

Ejemplo de definición de índice:

```sql
CREATE TABLE events
(
    id UInt64,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="json-all-values-supported-query-patterns">
  ##### Patrones de consulta admitidos
</div>

Una vez creado el índice, puede acelerar las consultas en subcolumnas JSON con las mismas funciones que se usan para las columnas `String` y con la función `equals` para todas las columnas.

Acceso a subcolumnas:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

Acceso a subcolumnas mediante `CAST` explícito:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

Operador `IN`:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### Búsqueda de frases
</div>

Una búsqueda normal con un índice de texto, por ejemplo

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

coincide con todas las filas que contienen los tokens indicados en cualquier orden.
En el ejemplo, la fila `While she stayed in Tokyo, the weather was great.` coincide con el filtro.

En cambio, la búsqueda de frases consiste en hacer coincidir los tokens en el orden indicado.
Por ejemplo,

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

coincide con cualquier fila que contenga la secuencia de tokens `weather in Tokyo`, como `How is the weather in Tokyo?`?

El índice de texto acelera la búsqueda de frases al intersectar las listas de postings de todos los tokens de la frase para identificar gránulos candidatos.
Dentro de esos gránulos, ClickHouse verifica la adyacencia exacta de los tokens.
Este proceso es relativamente costoso y más lento que las consultas habituales de búsqueda de texto.
Para acelerar las consultas de búsqueda de frases, habilite el almacenamiento de posiciones en el índice de texto (consulte `Optional parameters` más arriba).

`hasPhrase` puede usarse junto con los tokenizadores `splitByNonAlpha`, `splitByString`, `ngrams` y `asciiCJK`.
La cadena de la frase proporcionada se tokeniza con el tokenizador del índice.
Los caracteres separadores de la frase se ignoran: `hasPhrase(text, 'quick+brown')` es equivalente a `hasPhrase(text, 'quick brown')`, suponiendo que `splitByNonAlpha` se use como tokenizador.

<div id="text-index-phrase-search-example">
  #### Ejemplo
</div>

```sql
CREATE TABLE tab (
    id UInt32,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'weather in New York'),
    (2, 'New weather in York'),
    (3, 'weather in New Orleans');
```

```sql title="Query"
SELECT id, text FROM tab WHERE hasPhrase(text, 'weather in New York');
```

```result title="Response"
   ┌─id─┬─text────────────────┐
1. │  1 │ weather in New York │
   └────┴─────────────────────┘
```

La fila 2 (`'New weather in York'`) no coincide porque los tokens están en el orden equivocado.
La fila 3 (`'weather in New Orleans'`) no coincide porque no contiene el token `'York'`.

<div id="performance-tuning">
  ## Ajuste del rendimiento
</div>

<div id="direct-read">
  ### lectura directa
</div>

Ciertos tipos de consultas de texto pueden acelerarse considerablemente mediante una optimización llamada &quot;lectura directa&quot;.

Ejemplo:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

La optimización de lectura directa responde a la consulta exclusivamente mediante el índice de texto (es decir, búsquedas en el índice de texto), sin acceder a la columna de texto subyacente.
Las búsquedas en el índice de texto leen relativamente pocos datos y, por tanto, son mucho más rápidas que los skip indexes habituales de ClickHouse (que realizan una búsqueda en el skip index y, a continuación, cargan y filtran los gránulos restantes).

La lectura directa se controla mediante dos configuraciones:

* La configuración [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (`true` de forma predeterminada), que especifica si la lectura directa está habilitada de forma general.
* La configuración [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) era un requisito previo para la lectura directa en las versiones de ClickHouse &lt; 26.4.

**Funciones compatibles**

La optimización de lectura directa admite las funciones `hasToken`, `hasAllTokens` y `hasAnyTokens`.
Si el índice de texto está definido con un tokenizer `array`, la lectura directa también es compatible con las funciones `equals`, `has`, `hasAny`, `hasAll`, `mapContainsKey` y `mapContainsValue`.
Estas funciones también pueden combinarse mediante los operadores `AND`, `OR` y `NOT`.
Las cláusulas `WHERE` o `PREWHERE` también pueden contener filtros adicionales que no sean funciones de búsqueda de texto (para columnas de texto u otras columnas); en ese caso, la optimización de lectura directa seguirá utilizándose, pero será menos eficaz (solo se aplica a las funciones de búsqueda de texto compatibles).

Para comprobar si una consulta utiliza lectura directa, ejecute la consulta con `EXPLAIN PLAN actions = 1`.
Como ejemplo, una consulta con la lectura directa deshabilitada

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable lectura directa
```

devuelve

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

mientras que la misma consulta, ejecutada con `query_plan_direct_read_from_text_index = 1`

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable lectura directa
```

devuelve

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

La segunda salida de EXPLAIN PLAN contiene una columna virtual `__text_index_<index_name>_<function_name>_<id>`.
Si esta columna está presente, se utiliza la lectura directa.

Si la cláusula de filtro WHERE solo contiene funciones de búsqueda de texto, la consulta puede evitar por completo leer los datos de la columna y obtener el máximo beneficio de rendimiento con la lectura directa.
Sin embargo, aunque se acceda a la columna de texto en otra parte de la consulta, la lectura directa seguirá aportando una mejora del rendimiento.

**Lectura directa como sugerencia**

La lectura directa como sugerencia se basa en los mismos principios que la lectura directa normal, pero en este caso añade un filtro adicional construido a partir de los datos del índice de texto sin dejar de usar la columna de texto subyacente.
Se utiliza con funciones para las que leer solo desde el índice de texto produciría falsos positivos.

Las funciones compatibles son: `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey` y `mapContainsValue`.

El filtro adicional puede aportar más selectividad para restringir aún más el conjunto de resultados en combinación con otros filtros, lo que ayuda a reducir la cantidad de datos leídos de otras columnas.

La lectura directa como sugerencia se controla mediante la configuración [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (habilitada de forma predeterminada).

Ejemplo de consulta sin sugerencia:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

devuelve

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

mientras que la misma consulta se ejecuta con `query_plan_text_index_add_hint = 1`

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

devuelve

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

En la segunda salida de EXPLAIN PLAN, puede ver que se ha añadido una conjunción adicional (`__text_index_...`) a la condición de filtro.
Gracias a la optimización [PREWHERE](/es/sql-reference/statements/select/prewhere), la condición de filtro se descompone en tres conjunciones independientes, que se aplican en orden de complejidad computacional creciente.
Para esta consulta, el orden de aplicación es `__text_index_...`, luego `greaterOrEquals(...)` y, por último, `like(...)`.
Este orden permite omitir aún más gránulos de datos que los que ya omiten el índice de texto y el filtro original, antes de leer las columnas pesadas utilizadas en la consulta después de la cláusula `WHERE`, lo que reduce aún más la cantidad de datos que se debe leer.

<div id="like-ilike-queries-perf">
  ### Consultas LIKE/ILIKE
</div>

Cuando el patrón de una consulta LIKE/ILIKE es `%<alpha-numeric-characters-without-spaces>%` y el tokenizador del índice de texto es `splitByNonAlpha` o `array`, ClickHouse aprovecha el índice invertido para acelerar significativamente las consultas LIKE/ILIKE. Para ello, ClickHouse examina el diccionario del índice invertido en lugar de hacer un escaneo completo de la tabla para encontrar el patrón buscado.

Cuando la optimización está habilitada, las consultas LIKE/ILIKE deberían ser significativamente más rápidas que un escaneo completo de la tabla. Sin embargo, cuando el patrón coincide con la mayoría de los tokens del diccionario, el rendimiento puede ser peor que el de un escaneo completo de la tabla. Afortunadamente, existe un mecanismo de fallback para evitarlo.

La optimización se controla mediante una configuración:

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

El mecanismo de fallback se controla mediante dos configuraciones:

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

Esta optimización solo admite las funciones `like` e `ilike`.

<div id="caching">
  ### Almacenamiento en caché
</div>

Existen diferentes cachés a nivel de servidor para almacenar temporalmente en memoria partes del índice de texto (consulte la sección [Detalles de implementación](#implementation)):
Actualmente, hay cachés para los encabezados deserializados, los tokens y las lista de postings del índice de texto a fin de reducir la E/S.
Use las configuraciones [use&#95;text&#95;index&#95;header&#95;cache](/es/operations/settings/settings#use_text_index_header_cache), [use&#95;text&#95;index&#95;tokens&#95;cache](/es/operations/settings/settings#use_text_index_tokens_cache) y [use&#95;text&#95;index&#95;postings&#95;cache](/es/operations/settings/settings#use_text_index_postings_cache) para deshabilitar en las consultas la lectura y escritura en las cachés individuales.

Para borrar las cachés, use la sentencia [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

Consulte las siguientes configuraciones del servidor para ajustar las cachés.

<div id="caching-tokens">
  #### Configuración de la caché de tokens del índice de texto
</div>

| Configuración                                                                                                                                       | Descripción                                                                                                        |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/es/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | Nombre de la política de la caché de tokens del índice de texto.                                                   |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/es/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | Tamaño máximo de la caché en bytes.                                                                                |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/es/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | Número máximo de tokens deserializados en la caché.                                                                |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/es/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | Tamaño de la cola protegida de la caché de tokens del índice de texto en relación con el tamaño total de la caché. |

<div id="caching-header">
  #### Configuración de la caché de encabezados
</div>

| Setting                                                                                                                                             | Description                                                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/es/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | Nombre de la política de la caché de encabezados del índice de texto.                                                   |
| [text&#95;index&#95;header&#95;cache&#95;size](/es/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | Tamaño máximo de la caché en bytes.                                                                                     |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/es/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | Número máximo de encabezados deserializados en caché.                                                                   |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/es/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | Tamaño de la cola protegida en la caché de encabezados del índice de texto en relación con el tamaño total de la caché. |

<div id="caching-posting-lists">
  #### Configuración de la caché de listas de postings
</div>

| Configuración                                                                                                                                           | Descripción                                                                                                          |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/es/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | Nombre de la política de caché de postings del índice de texto.                                                      |
| [text&#95;index&#95;postings&#95;cache&#95;size](/es/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | Tamaño máximo de la caché en bytes.                                                                                  |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/es/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | Número máximo de postings deserializados en la caché.                                                                |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/es/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | Tamaño de la cola protegida en la caché de postings del índice de texto en relación con el tamaño total de la caché. |

<div id="limitations">
  ## Limitaciones
</div>

Actualmente, el índice de texto tiene las siguientes limitaciones:

* La materialización de índices de texto con un gran número de tokens (p. ej., 10 mil millones de tokens) puede consumir cantidades significativas de memoria. La materialización del índice de texto
  puede producirse directamente (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) o indirectamente durante las fusiones de partes.
* No es posible materializar índices de texto en partes con más de 4.294.967.296 (= 2^32 = aprox. 4,2 mil millones) filas. Sin un índice de texto materializado, las consultas pasan a una búsqueda lenta por fuerza bruta dentro de la parte. Como estimación del peor caso, suponga que una parte contiene una sola columna de tipo String y que la configuración de MergeTree `max_bytes_to_merge_at_max_space_in_pool` (valor predeterminado: 150 GB) no se ha modificado. En este caso, la situación se da si la columna contiene, en promedio, menos de 29,5 caracteres por fila. En la práctica, las tablas también contienen otras columnas, y el umbral es varias veces menor (dependiendo del número, tipo y tamaño de las demás columnas).

<div id="text-index-vs-bloom-filter-indexes">
  ## Índices de texto frente a índices basados en filtros Bloom
</div>

Los predicados sobre cadenas pueden acelerarse mediante índices de texto e índices basados en filtros Bloom (tipo de índice `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`), aunque ambos difieren fundamentalmente en su diseño y en los casos de uso para los que fueron concebidos:

**Índices de filtro Bloom**

* Se basan en estructuras de datos probabilísticas que pueden producir falsos positivos.
* Solo pueden responder preguntas de pertenencia a conjuntos; es decir, si la columna puede contener el token X o si definitivamente no lo contiene.
* Almacenan información a nivel de gránulo para permitir omitir rangos amplios durante la ejecución de la consulta.
* Son difíciles de ajustar correctamente (consulta [aquí](mergetree#n-gram-bloom-filter) un ejemplo).
* Son relativamente compactos (unos pocos kilobytes o megabytes por parte).

**Índices de texto**

* Construyen un índice invertido determinista sobre tokens. El propio índice no puede producir falsos positivos.
* Están optimizados específicamente para cargas de trabajo de búsqueda de texto completo.
* Almacenan información a nivel de fila, lo que permite buscar términos de forma eficiente.
* Son bastante grandes (de decenas a cientos de megabytes por parte).

Los índices basados en filtros Bloom admiten la búsqueda de texto completo solo como un &quot;efecto secundario&quot;:

* No admiten tokenización ni preprocesamiento avanzados.
* No admiten búsquedas de varios tokens.
* No ofrecen las características de rendimiento esperadas de un índice invertido.

Los índices de texto, en cambio, están diseñados específicamente para la búsqueda de texto completo:

* Proporcionan tokenización y preprocesamiento
* Ofrecen soporte eficiente para `hasAllTokens`, `LIKE`, `match` y funciones similares de búsqueda de texto.
* Tienen una escalabilidad significativamente mejor para grandes corpus de texto.

<div id="implementation">
  ## Detalles de implementación
</div>

Cada índice de texto consta de dos estructuras de datos (abstractas):

* un diccionario que asigna cada token a una lista de postings, y
* un conjunto de listas de postings, cada una de las cuales representa un conjunto de números de fila.

El índice de texto se crea para toda la parte.
A diferencia de otros índices de omisión, el índice de texto puede fusionarse en lugar de reconstruirse durante la fusión de las partes de datos (véase más abajo).

Durante la creación del índice, se crean tres archivos (por parte):

**Archivo de bloques del diccionario (.dct)**

Los tokens del índice de texto se ordenan y se almacenan en bloques de diccionario de 512 tokens cada uno (el tamaño del bloque se puede configurar mediante el parámetro `dictionary_block_size`).
Un archivo de bloques del diccionario (.dct) contiene todos los bloques de diccionario de todos los gránulos de índice de una parte.

**Archivo de cabecera del índice (.idx)**

El archivo de cabecera del índice contiene, para cada bloque de diccionario, el primer token del bloque y su desplazamiento relativo dentro del archivo de bloques del diccionario.

Esta estructura de índice disperso es similar al [índice primario disperso](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) de ClickHouse.

**Archivo de listas de postings (.pst)**

Las listas de postings de todos los tokens se organizan secuencialmente en el archivo de listas de postings.
Para ahorrar espacio y, al mismo tiempo, permitir operaciones rápidas de intersección y unión, las listas de postings se almacenan como [roaring bitmaps](https://roaringbitmap.org/).
Si la lista de postings es mayor que `posting_list_block_size`, se divide en varios bloques que se almacenan secuencialmente en el archivo de listas de postings.

**Archivo de posiciones (.pos)**

Opcional, solo si el argumento del índice `positions = 1`.
Almacena las posiciones de los tokens dentro de las filas coincidentes.

**Fusión de índices de texto**

Cuando se fusionan partes de datos, no es necesario reconstruir el índice de texto desde cero; en su lugar, puede fusionarse de forma eficiente en un paso independiente del proceso de fusión.
Durante este paso, los diccionarios ordenados de los índices de texto de cada parte de entrada se leen y se combinan en un nuevo diccionario unificado.
Los números de fila de las listas de postings también se recalculan para reflejar sus nuevas posiciones en la parte de datos fusionada, usando una correspondencia entre números de fila antiguos y nuevos que se crea durante la fase inicial de fusión.
Este método de fusionar índices de texto es similar a cómo se fusionan las [projections](/es/docs/sql-reference/statements/alter/projection#projection-indexes) con la columna `_part_offset`.
Si el índice no está materializado en la parte de origen, se crea, se escribe en un archivo temporal y luego se fusiona junto con los índices de las otras partes y de otros archivos de índice temporales.

**Depuración**

La función de tabla [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) puede usarse para inspeccionar índices de texto.

<div id="hacker-news-dataset">
  ## Ejemplo: conjunto de datos de Hacker News
</div>

Veamos las mejoras del rendimiento de los índices de texto en un conjunto de datos grande con mucho contenido textual.
Usaremos 28,7 M de filas de comentarios del popular sitio web Hacker News.
Aquí está la tabla sin índice de texto:

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

Los 28,7 millones de filas están en un archivo Parquet en S3; vamos a insertarlas en la tabla `hackernews`:

```sql
INSERT INTO hackernews
    SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

Usaremos `ALTER TABLE` y agregaremos un índice de texto en la columna `comment`; luego, lo materializaremos:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

Ahora, ejecutemos consultas con las funciones `hasToken`, `hasAnyTokens` y `hasAllTokens`.
Los siguientes ejemplos mostrarán la marcada diferencia de rendimiento entre un escaneo estándar del índice y la optimización de lectura directa.

<div id="using-hasToken">
  ### 1. Uso de `hasToken`
</div>

`hasToken` comprueba si el texto contiene un único token específico.
Buscaremos el token sensible a mayúsculas y minúsculas &#39;ClickHouse&#39;.

**Lectura directa deshabilitada (Escaneo estándar)**
De forma predeterminada, ClickHouse usa el índice de omisión para filtrar gránulos y luego lee los datos de la columna de esos gránulos.
Podemos simular este comportamiento deshabilitando la lectura directa.

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.362 sec. Processed 24.90 million rows, 9.51 GB
```

**Lectura directa activada (lectura rápida del índice)**
Ahora ejecutamos la misma consulta con la lectura directa activada (opción predeterminada).

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.008 sec. Processed 3.15 million rows, 3.15 MB
```

La consulta con lectura directa es más de 45 veces más rápida (0.362s frente a 0.008s) y procesa muchos menos datos (9.51 GB frente a 3.15 MB), ya que lee únicamente del índice.

<div id="using-hasAnyTokens">
  ### 2. Uso de `hasAnyTokens`
</div>

`hasAnyTokens` comprueba si el texto contiene al menos uno de los tokens indicados.
Buscaremos comentarios que contengan &#39;love&#39; o &#39;ClickHouse&#39;.

**Lectura directa deshabilitada (escaneo estándar)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 1.329 sec. Processed 28.74 million rows, 9.72 GB
```

**Lectura directa activada (Lectura rápida del índice)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 0.015 sec. Processed 27.99 million rows, 27.99 MB
```

La mejora de velocidad es aún más espectacular para esta búsqueda común con &quot;OR&quot;.
La consulta es casi 89 veces más rápida (1.329s frente a 0.015s) al evitar el escaneo completo de la columna.

<div id="using-hasAllTokens">
  ### 3. Uso de `hasAllTokens`
</div>

`hasAllTokens` comprueba si el texto contiene todos los tokens indicados.
Buscaremos comentarios que contengan tanto &#39;love&#39; como &#39;ClickHouse&#39;.

**Lectura directa deshabilitada (Escaneo estándar)**
Incluso con la lectura directa deshabilitada, el índice de omisión estándar sigue siendo eficaz.
Reduce las 28.7M filas a solo 147.46K, pero aun así debe leer 57.03 MB de la columna.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.184 sec. Processed 147.46 thousand rows, 57.03 MB
```

**Lectura directa habilitada (lectura rápida del índice)**
La lectura directa resuelve la consulta usando los datos del índice y solo lee 147.46 KB.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.007 sec. Processed 147.46 thousand rows, 147.46 KB
```

Para esta búsqueda &quot;AND&quot;, la optimización de lectura directa es más de 26 veces más rápida (0.184s frente a 0.007s) que el escaneo estándar del índice de omisión.

<div id="compound-search">
  ### 4. Búsqueda compuesta: OR, AND, NOT, ...
</div>

La optimización de lectura directa también se aplica a las expresiones booleanas compuestas.
Aquí, haremos una búsqueda de &#39;ClickHouse&#39; OR &#39;clickhouse&#39; sin distinguir entre mayúsculas y minúsculas.

**Direct read deshabilitado (escaneo estándar)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.450 sec. Processed 25.87 million rows, 9.58 GB
```

**Lectura directa activada (Lectura rápida del índice)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.013 sec. Processed 25.87 million rows, 51.73 MB
```

Al combinar los resultados del índice, la consulta con lectura directa es 34 veces más rápida (0.450s frente a 0.013s) y evita leer 9.58 GB de datos de columnas.
Para este caso concreto, `hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])` sería la sintaxis preferida y más eficiente.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Anuncio de la disponibilidad general de la búsqueda de texto completo de ClickHouse](https://clickhouse.com/blog/full-text-search-ga-release)
* Blog: [Cómo crear una búsqueda de texto completo de alto rendimiento para almacenamiento de objetos](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* Vídeo: [Introducción a la búsqueda de texto completo en ClickHouse](https://www.youtube.com/watch?v=9zPmf1a_heU)
* Vídeo: [Entre bastidores: la búsqueda de texto completo con la escala y velocidad de ClickHouse](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* Presentación: [La búsqueda de texto completo de ClickHouse por dentro: rápida, nativa y columnar](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* Presentación: [Índices invertidos en bases de datos: el porqué, el qué y el cómo, FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**Material desactualizado**

* Blog: [Presentamos los índices invertidos en ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* Blog: [La búsqueda de texto completo de ClickHouse por dentro: rápida, nativa y columnar](https://clickhouse.com/blog/clickhouse-full-text-search)
* Vídeo: [Índices de texto completo: diseño y experimentos](https://www.youtube.com/watch?v=O_MnyUkrIq8)