---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: Para reemplazar en cadenas
---

# Funciones para buscar y reemplazar en cadenas {#functions-for-searching-and-replacing-in-strings}

## replaceOne(pajar, patrón, reemplazo) {#replaceonehaystack-pattern-replacement}

Sustituye la primera aparición, si existe, de la ‘pattern’ subcadena en ‘haystack’ con el ‘replacement’ subcadena.
Sucesivo, ‘pattern’ y ‘replacement’ deben ser constantes.

## replaceAll (pajar, patrón, reemplazo), replace (pajar, patrón, reemplazo) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

Sustituye todas las apariciones del ‘pattern’ subcadena en ‘haystack’ con el ‘replacement’ subcadena.

## replaceRegexpOne (pajar, patrón, reemplazo) {#replaceregexponehaystack-pattern-replacement}

Reemplazo usando el ‘pattern’ expresión regular. Una expresión regular re2.
Sustituye sólo la primera ocurrencia, si existe.
Un patrón se puede especificar como ‘replacement’. Este patrón puede incluir sustituciones `\0-\9`.
Sustitución `\0` incluye toda la expresión regular. Sustitución `\1-\9` corresponden a los números de subpatrón. `\` en una plantilla, escapar de ella usando `\`.
También tenga en cuenta que un literal de cadena requiere un escape adicional.

Ejemplo 1. Conversión de la fecha a formato americano:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Ejemplo 2. Copiar una cadena diez veces:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll (pajar, patrón, reemplazo) {#replaceregexpallhaystack-pattern-replacement}

Esto hace lo mismo, pero reemplaza todas las ocurrencias. Ejemplo:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

Como excepción, si una expresión regular funcionó en una subcadena vacía, el reemplazo no se realiza más de una vez.
Ejemplo:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## Sistema abierto.) {#regexpquotemetas}

La función agrega una barra invertida antes de algunos caracteres predefinidos en la cadena.
Caracteres predefinidos: ‘0’, ‘\\’, ‘\|’, ‘(’, ‘)’, ‘^’, ‘$’, ‘.’, ‘\[’, '\]', ‘?’, '\*‘,’+‘,’{‘,’:‘,’-'.
Esta implementación difiere ligeramente de re2::RE2::QuoteMeta. Escapa de byte cero como \\0 en lugar de 00 y escapa solo de los caracteres requeridos.
Para obtener más información, consulte el enlace: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
