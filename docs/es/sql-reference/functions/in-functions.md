---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 60
toc_title: "Implementaci\xF3n del operador IN"
---

# Funciones Para Implementar El Operador IN {#functions-for-implementing-the-in-operator}

## Información De Uso {#in-functions}

Vea la sección [IN operadores](../statements/select.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

Una función que permite agrupar varias columnas.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
Las tuplas se usan normalmente como valores intermedios para un argumento de operadores IN, o para crear una lista de parámetros formales de funciones lambda. Las tuplas no se pueden escribir en una tabla.

## Puede Utilizar El Siguiente Ejemplo: {#tupleelementtuple-n-operator-x-n}

Una función que permite obtener una columna de una tupla.
‘N’ es el índice de columna, comenzando desde 1. N debe ser una constante. ‘N’ debe ser una constante. ‘N’ debe ser un entero postivo estricto no mayor que el tamaño de la tupla.
No hay ningún costo para ejecutar la función.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
