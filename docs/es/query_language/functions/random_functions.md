# Funciones para generar números pseudoaleatorios {#functions-for-generating-pseudo-random-numbers}

Se utilizan generadores no criptográficos de números pseudoaleatorios.

Todas las funciones aceptan cero argumentos o un argumento.
Si se pasa un argumento, puede ser de cualquier tipo y su valor no se usa para nada.
El único propósito de este argumento es evitar la eliminación de subexpresiones comunes, de modo que dos instancias diferentes de la misma función devuelvan columnas diferentes con números aleatorios diferentes.

## rand {#rand}

Devuelve un número pseudoaleatorio UInt32, distribuido uniformemente entre todos los números de tipo UInt32.
Utiliza un generador congruente lineal.

## rand64 {#rand64}

Devuelve un número pseudoaleatorio UInt64, distribuido uniformemente entre todos los números de tipo UInt64.
Utiliza un generador congruente lineal.

## randConstant {#randconstant}

Devuelve un número pseudoaleatorio UInt32, El valor es uno para diferentes bloques.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/random_functions/) <!--hide-->
