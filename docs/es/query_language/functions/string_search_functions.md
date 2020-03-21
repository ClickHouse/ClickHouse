# Funciones para buscar cadenas {#functions-for-searching-strings}

La búsqueda distingue entre mayúsculas y minúsculas de forma predeterminada en todas estas funciones. Hay variantes separadas para la búsqueda insensible a mayúsculas y minúsculas.

## posición (pajar, aguja), localizar (pajar, aguja) {#position}

Devuelve la posición (en bytes) de la subcadena encontrada en la cadena, comenzando desde 1.

Funciona bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado de un solo byte. Si no se cumple esta suposición y un carácter no se puede representar con un solo byte, la función no produce una excepción y devuelve algún resultado inesperado. Si el carácter se puede representar usando dos bytes, usará dos bytes y así sucesivamente.

Para una búsqueda sin distinción de mayúsculas y minúsculas, utilice la función [positionCaseInsensitive](#positioncaseinsensitive).

**Sintaxis**

``` sql
position(haystack, needle)
```

Apodo: `locate(haystack, needle)`.

**Parámetros**

-   `haystack` — cadena, en la que se buscará la subcadena. [Cadena](../syntax.md#syntax-string-literal).
-   `needle` — subcadena que se va a buscar. [Cadena](../syntax.md#syntax-string-literal).

**Valores devueltos**

-   Posición inicial en bytes (contando desde 1), si se encontró subcadena.
-   0, si no se encontró la subcadena.

Tipo: `Integer`.

**Ejemplos**

Frase «Hello, world!» contiene un conjunto de bytes que representan un texto codificado de un solo byte. La función devuelve algún resultado esperado:

Consulta:

``` sql
SELECT position('Hello, world!', '!')
```

Resultado:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

La misma frase en ruso contiene caracteres que no se pueden representar usando un solo byte. La función devuelve algún resultado inesperado (uso [PosiciónUTF8](#positionutf8) función para texto codificado de varios bytes):

Consulta:

``` sql
SELECT position('Привет, мир!', '!')
```

Resultado:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## positionCaseInsensitive {#positioncaseinsensitive}

Lo mismo que [posición](#position) devuelve la posición (en bytes) de la subcadena encontrada en la cadena, comenzando desde 1. Utilice la función para una búsqueda que no distingue entre mayúsculas y minúsculas.

Funciona bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado de un solo byte. Si no se cumple esta suposición y un carácter no se puede representar con un solo byte, la función no produce una excepción y devuelve algún resultado inesperado. Si el carácter se puede representar usando dos bytes, usará dos bytes y así sucesivamente.

**Sintaxis**

``` sql
positionCaseInsensitive(haystack, needle)
```

**Parámetros**

-   `haystack` — cadena, en la que se buscará la subcadena. [Cadena](../syntax.md#syntax-string-literal).
-   `needle` — subcadena que se va a buscar. [Cadena](../syntax.md#syntax-string-literal).

**Valores devueltos**

-   Posición inicial en bytes (contando desde 1), si se encontró subcadena.
-   0, si no se encontró la subcadena.

Tipo: `Integer`.

**Ejemplo**

Consulta:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

Resultado:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## PosiciónUTF8 {#positionutf8}

Devuelve la posición (en puntos Unicode) de la subcadena encontrada en la cadena, comenzando desde 1.

Funciona bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado en UTF-8. Si no se cumple esta suposición, la función no produce una excepción y devuelve algún resultado inesperado. Si el carácter se puede representar usando dos puntos Unicode, usará dos y así sucesivamente.

Para una búsqueda sin distinción de mayúsculas y minúsculas, utilice la función [PosiciónCasoInsensitiveUTF8](#positioncaseinsensitiveutf8).

**Sintaxis**

``` sql
positionUTF8(haystack, needle)
```

**Parámetros**

-   `haystack` — cadena, en la que se buscará la subcadena. [Cadena](../syntax.md#syntax-string-literal).
-   `needle` — subcadena que se va a buscar. [Cadena](../syntax.md#syntax-string-literal).

**Valores devueltos**

-   Posición inicial en puntos Unicode (contando desde 1), si se encontró subcadena.
-   0, si no se encontró la subcadena.

Tipo: `Integer`.

**Ejemplos**

Frase «Hello, world!» en ruso contiene un conjunto de puntos Unicode que representan un texto codificado de un solo punto. La función devuelve algún resultado esperado:

Consulta:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

Resultado:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

Frase «Salut, étudiante!», donde el carácter `é` Puede ser representado usando un punto (`U+00E9`) o dos puntos (`U+0065U+0301`) la función se puede devolver algún resultado inesperado:

Consulta de la carta `é`, que se representa un punto Unicode `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Resultado:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

Consulta de la carta `é`, que se representa dos puntos Unicode `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Resultado:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## PosiciónCasoInsensitiveUTF8 {#positioncaseinsensitiveutf8}

Lo mismo que [PosiciónUTF8](#positionutf8), pero no distingue entre mayúsculas y minúsculas. Devuelve la posición (en puntos Unicode) de la subcadena encontrada en la cadena, comenzando desde 1.

Funciona bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado en UTF-8. Si no se cumple esta suposición, la función no produce una excepción y devuelve algún resultado inesperado. Si el carácter se puede representar usando dos puntos Unicode, usará dos y así sucesivamente.

**Sintaxis**

``` sql
positionCaseInsensitiveUTF8(haystack, needle)
```

**Parámetros**

-   `haystack` — cadena, en la que se buscará la subcadena. [Cadena](../syntax.md#syntax-string-literal).
-   `needle` — subcadena que se va a buscar. [Cadena](../syntax.md#syntax-string-literal).

**Valor devuelto**

-   Posición inicial en puntos Unicode (contando desde 1), si se encontró subcadena.
-   0, si no se encontró la subcadena.

Tipo: `Integer`.

**Ejemplo**

Consulta:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

Resultado:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## multiSearchAllPositions {#multisearchallpositions}

Lo mismo que [posición](string_search_functions.md#position) pero devuelve `Array` posiciones (en bytes) de las subcadenas correspondientes encontradas en la cadena. Las posiciones se indexan a partir de 1.

La búsqueda se realiza en secuencias de bytes sin respecto a la codificación de cadenas y la intercalación.

-   Para la búsqueda ASCII sin distinción de mayúsculas y minúsculas, utilice la función `multiSearchAllPositionsCaseInsensitive`.
-   Para buscar en UTF-8, use la función [MultiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   Para la búsqueda UTF-8 sin distinción de mayúsculas y minúsculas, utilice la función multiSearchAllPositionsCaseInsensitiveUTF8.

**Sintaxis**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**Parámetros**

-   `haystack` — cadena, en la que se buscará la subcadena. [Cadena](../syntax.md#syntax-string-literal).
-   `needle` — subcadena que se va a buscar. [Cadena](../syntax.md#syntax-string-literal).

**Valores devueltos**

-   Matriz de posiciones iniciales en bytes (contando desde 1), si se encontró la subcadena correspondiente y 0 si no se encuentra.

**Ejemplo**

Consulta:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

Resultado:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## MultiSearchAllPositionsUTF8 {#multisearchallpositionsutf8}

Ver `multiSearchAllPositions`.

## multiSearchFirstPosition(pajar, \[aguja<sub>Uno</sub>, aguja<sub>Cómo hacer</sub>, …, aguja<sub>y</sub>\]) {#multisearchfirstposition}

Lo mismo que `position` pero devuelve el desplazamiento más a la izquierda de la cadena `haystack` que se corresponde con algunas de las agujas.

Para una búsqueda que no distingue entre mayúsculas y minúsculas o / y en formato UTF-8, use funciones `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstIndex(pajar, \[aguja<sub>Uno</sub>, aguja<sub>Cómo hacer</sub>, …, aguja<sub>y</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

Devuelve el índice `i` (a partir de 1) de la aguja encontrada más a la izquierda<sub>me</sub> es la cadena `haystack` y 0 de lo contrario.

Para una búsqueda que no distingue entre mayúsculas y minúsculas o / y en formato UTF-8, use funciones `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny(pajar, \[aguja<sub>Uno</sub>, aguja<sub>Cómo hacer</sub>, …, aguja<sub>y</sub>\]) {#function-multisearchany}

Devuelve 1, si al menos una aguja de cuerda<sub>me</sub> coincide con la cadena `haystack` y 0 de lo contrario.

Para una búsqueda que no distingue entre mayúsculas y minúsculas o / y en formato UTF-8, use funciones `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "Nota"
    En todos `multiSearch*` el número de agujas debe ser inferior a 2<sup>Ocho</sup> debido a la especificación de implementación.

## match (pajar, patrón) {#matchhaystack-pattern}

Comprueba si la cadena coincide con la `pattern` expresión regular. Un `re2` expresión regular. El [sintaxis](https://github.com/google/re2/wiki/Syntax) de la `re2` expresiones regulares es más limitada que la sintaxis de las expresiones regulares de Perl.

Devuelve 0 si no coinciden, o 1 si coinciden.

Tenga en cuenta que el símbolo de barra invertida (`\`) se utiliza para escapar en la expresión regular. El mismo símbolo se usa para escapar en literales de cadena. Por lo tanto, para escapar del símbolo en una expresión regular, debe escribir dos barras invertidas (\\) en un literal de cadena.

La expresión regular funciona con la cadena como si fuera un conjunto de bytes. La expresión regular no puede contener bytes nulos.
Para que los patrones busquen subcadenas en una cadena, es mejor usar LIKE o ‘position’, ya que trabajan mucho más rápido.

## multiMatchAny(pajar, \[patrón<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

Lo mismo que `match`, pero devuelve 0 si ninguna de las expresiones regulares coinciden y 1 si alguno de los patrones coinciden. Se utiliza [hyperscan](https://github.com/intel/hyperscan) biblioteca. Para que los patrones busquen subcadenas en una cadena, es mejor usar `multiSearchAny` ya que funciona mucho más rápido.

!!! note "Nota"
    La longitud de cualquiera de los `haystack` Cadena debe ser inferior a 2<sup>Nivel de Cifrado WEP</sup> bytes de lo contrario, se lanza la excepción. Esta restricción tiene lugar debido a la API de hiperscan.

## multiMatchAnyIndex(pajar, \[patrón<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

Lo mismo que `multiMatchAny`, pero devuelve cualquier índice que coincida con el pajar.

## ¿Cómo puedo obtener más información?<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

Lo mismo que `multiMatchAny`, pero devuelve la matriz de todas las indicaciones que coinciden con el pajar en cualquier orden.

## multiFuzzyMatchAny(pajar, distancia, \[patrón<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

Lo mismo que `multiMatchAny`, pero devuelve 1 si algún patrón coincide con el pajar dentro de una constante [Editar distancia](https://en.wikipedia.org/wiki/Edit_distance). Esta función también está en modo experimental y puede ser extremadamente lenta. Para obtener más información, consulte [documentación de hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multiFuzzyMatchAnyIndex(pajar, distancia, \[patrón<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

Lo mismo que `multiFuzzyMatchAny`, pero devuelve cualquier índice que coincida con el pajar dentro de una distancia de edición constante.

## multiFuzzyMatchAllIndices(pajar, distancia, \[patrón<sub>Uno</sub>, patrón<sub>Cómo hacer</sub>, …, patrón<sub>y</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

Lo mismo que `multiFuzzyMatchAny`, pero devuelve la matriz de todos los índices en cualquier orden que coincida con el pajar dentro de una distancia de edición constante.

!!! note "Nota"
    `multiFuzzyMatch*` las funciones no admiten expresiones regulares UTF-8, y dichas expresiones se tratan como bytes debido a la restricción de hiperscan.

!!! note "Nota"
    Para desactivar todas las funciones que utilizan hyperscan, utilice la configuración `SET allow_hyperscan = 0;`.

## extracto(pajar, patrón) {#extracthaystack-pattern}

Extrae un fragmento de una cadena utilizando una expresión regular. Si ‘haystack’ no coinciden con el ‘pattern’ regex, se devuelve una cadena vacía. Si la expresión regular no contiene subpatrones, toma el fragmento que coincide con toda la expresión regular. De lo contrario, toma el fragmento que coincide con el primer subpatrón.

## extractAll(pajar, patrón) {#extractallhaystack-pattern}

Extrae todos los fragmentos de una cadena utilizando una expresión regular. Si ‘haystack’ no coinciden con el ‘pattern’ regex, se devuelve una cadena vacía. Devuelve una matriz de cadenas que consiste en todas las coincidencias con la expresión regular. En general, el comportamiento es el mismo que el ‘extract’ función (toma el primer subpatrón, o la expresión completa si no hay un subpatrón).

## como (pajar, patrón), operador de patrón COMO pajar {#function-like}

Comprueba si una cadena coincide con una expresión regular simple.
La expresión regular puede contener los metasímbolos `%` y `_`.

`%` indica cualquier cantidad de bytes (incluidos cero caracteres).

`_` indica cualquier byte.

Utilice la barra invertida (`\`) para escapar de metasímbolos. Vea la nota sobre el escape en la descripción del ‘match’ función.

Para expresiones regulares como `%needle%`, el código es más óptimo y trabaja tan rápido como el `position` función.
Para otras expresiones regulares, el código es el mismo que para ‘match’ función.

## notLike(haystack, pattern), haystack NOT LIKE operador de patrón {#function-notlike}

Lo mismo que ‘like’ pero negativo.

## ngramDistance(pajar, aguja) {#ngramdistancehaystack-needle}

Calcula la distancia de 4 gramos entre `haystack` y `needle`: cuenta la diferencia simétrica entre dos conjuntos múltiples de 4 gramos y la normaliza por la suma de sus cardinalidades. Devuelve un número flotante de 0 a 1: cuanto más cerca de cero, más cadenas son similares entre sí. Si la constante `needle` o `haystack` es más de 32Kb, arroja una excepción. Si algunos de los no constantes `haystack` o `needle` Las cadenas son más de 32Kb, la distancia es siempre una.

Para la búsqueda sin distinción de mayúsculas y minúsculas o / y en formato UTF-8, use funciones `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramSearch(pajar, aguja) {#ngramsearchhaystack-needle}

Lo mismo que `ngramDistance` pero calcula la diferencia no simétrica entre `needle` y `haystack` – el número de n-grams de la aguja menos el número común de n-grams normalizado por el número de `needle` n-gramas. Cuanto más cerca de uno, más probable es `needle` está en el `haystack`. Puede ser útil para la búsqueda de cadenas difusas.

Para la búsqueda sin distinción de mayúsculas y minúsculas o / y en formato UTF-8, use funciones `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "Nota"
    Para el caso UTF-8 usamos una distancia de 3 gramos. Todas estas no son distancias n-gram perfectamente justas. Usamos hashes de 2 bytes para hash n-gramos y luego calculamos la diferencia (no) simétrica entre estas tablas hash: pueden ocurrir colisiones. Con el formato UTF-8 sin distinción de mayúsculas y minúsculas, no usamos fair `tolower` función - ponemos a cero el bit 5-th (comenzando desde cero) de cada byte de punto de código y el primer bit de byte zeroth si bytes más de uno - esto funciona para el latín y principalmente para todas las letras cirílicas.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/string_search_functions/) <!--hide-->
