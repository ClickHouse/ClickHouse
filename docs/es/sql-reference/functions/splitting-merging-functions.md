---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 47
toc_title: "Divisi\xF3n y fusi\xF3n de cuerdas y matrices"
---

# Funciones Para Dividir y Fusionar Cadenas y Matrices {#functions-for-splitting-and-merging-strings-and-arrays}

## Por Ejemplo:) {#splitbycharseparator-s}

Divide una cadena en subcadenas separadas por un carácter especificado. Utiliza una cadena constante `separator` que consiste en exactamente un carácter.
Devuelve una matriz de subcadenas seleccionadas. Se pueden seleccionar subcadenas vacías si el separador aparece al principio o al final de la cadena, o si hay varios separadores consecutivos.

**Sintaxis**

``` sql
splitByChar(<separator>, <s>)
```

**Parámetros**

-   `separator` — The separator which should contain exactly one character. [Cadena](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Cadena](../../sql-reference/data-types/string.md).

**Valores devueltos)**

Devuelve una matriz de subcadenas seleccionadas. Las subcadenas vacías se pueden seleccionar cuando:

-   Se produce un separador al principio o al final de la cadena;
-   Hay varios separadores consecutivos;
-   La cadena original `s` está vacío.

Tipo: [Matriz](../../sql-reference/data-types/array.md) de [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## Por Ejemplo:) {#splitbystringseparator-s}

Divide una cadena en subcadenas separadas por una cadena. Utiliza una cadena constante `separator` de múltiples caracteres como separador. Si la cadena `separator` está vacío, dividirá la cadena `s` en una matriz de caracteres individuales.

**Sintaxis**

``` sql
splitByString(<separator>, <s>)
```

**Parámetros**

-   `separator` — The separator. [Cadena](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Cadena](../../sql-reference/data-types/string.md).

**Valores devueltos)**

Devuelve una matriz de subcadenas seleccionadas. Las subcadenas vacías se pueden seleccionar cuando:

Tipo: [Matriz](../../sql-reference/data-types/array.md) de [Cadena](../../sql-reference/data-types/string.md).

-   Se produce un separador no vacío al principio o al final de la cadena;
-   Hay varios separadores consecutivos no vacíos;
-   La cadena original `s` está vacío mientras el separador no está vacío.

**Ejemplo**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde')
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## Por Ejemplo, Se Puede Usar Una Matriz.\]) {#arraystringconcatarr-separator}

Concatena las cadenas enumeradas en la matriz con el separador.’separador’ es un parámetro opcional: una constante de cadena, establece una cadena vacía por defecto.
Devuelve la cadena.

## Sistema Abierto.) {#alphatokenss}

Selecciona subcadenas de bytes consecutivos de los rangos a-z y A-Z.Devuelve una matriz de subcadenas.

**Ejemplo**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
