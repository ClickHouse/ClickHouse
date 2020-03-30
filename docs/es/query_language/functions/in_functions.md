---
machine_translated: true
---

# Funciones para implementar el operador IN {#functions-for-implementing-the-in-operator}

## Información de uso {#in-functions}

Vea la sección [IN operadores](../select.md#select-in-operators).

## tuple(x, y, …), operador (x, y, …) {#tuplex-y-operator-x-y}

Una función que permite agrupar varias columnas.
Para columnas con los tipos T1, T2, …, devuelve una tupla de tipo Tuple(T1, T2, …) que contiene estas columnas. No hay ningún costo para ejecutar la función.
Las tuplas se usan normalmente como valores intermedios para un argumento de operadores IN, o para crear una lista de parámetros formales de funciones lambda. Las tuplas no se pueden escribir en una tabla.

## Puede utilizar el siguiente ejemplo: {#tupleelementtuple-n-operator-x-n}

Una función que permite obtener una columna de una tupla.
‘N’ es el índice de columna, comenzando desde 1. N debe ser una constante. ‘N’ debe ser una constante. ‘N’ debe ser un entero postivo estricto no mayor que el tamaño de la tupla.
No hay ningún costo para ejecutar la función.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/in_functions/) <!--hide-->
