# Funciones para dividir y fusionar cadenas y matrices {#functions-for-splitting-and-merging-strings-and-arrays}

## Por ejemplo:) {#splitbycharseparator-s}

Divide una cadena en subcadenas separadas por ‘separator’.’separador’ debe ser una constante de cadena que consta de exactamente un carácter.
Devuelve una matriz de subcadenas seleccionadas. Se pueden seleccionar subcadenas vacías si el separador aparece al principio o al final de la cadena, o si hay varios separadores consecutivos.

**Ejemplo:**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## Por ejemplo:) {#splitbystringseparator-s}

Lo mismo que el anterior, pero usa una cadena de múltiples caracteres como separador. Si la cadena está vacía, dividirá la cadena en una matriz de caracteres individuales.

**Ejemplo:**

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

## Por ejemplo, se puede usar una matriz.\]) {#arraystringconcatarr-separator}

Concatena las cadenas enumeradas en la matriz con el separador.’separador’ es un parámetro opcional: una cadena constante, establecida en una cadena vacía por defecto.
Devuelve la cadena.

## Sistema abierto.) {#alphatokenss}

Selecciona subcadenas de bytes consecutivos de los rangos a-z y A-Z.Devuelve una matriz de subcadenas.

**Ejemplo:**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/splitting_merging_functions/) <!--hide-->
