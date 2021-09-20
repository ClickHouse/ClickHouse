---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: Tuple (T1, T2, ...)
---

# Tuple(t1, T2, …) {#tuplet1-t2}

Una tupla de elementos, cada uno con un individuo [tipo](index.md#data_types).

Las tuplas se utilizan para la agrupación temporal de columnas. Las columnas se pueden agrupar cuando se usa una expresión IN en una consulta y para especificar ciertos parámetros formales de las funciones lambda. Para obtener más información, consulte las secciones [IN operadores](../../sql-reference/operators/in.md) y [Funciones de orden superior](../../sql-reference/functions/higher-order-functions.md).

Las tuplas pueden ser el resultado de una consulta. En este caso, para formatos de texto distintos de JSON, los valores están separados por comas entre corchetes. En formatos JSON, las tuplas se generan como matrices (entre corchetes).

## Creación de una tupla {#creating-a-tuple}

Puedes usar una función para crear una tupla:

``` sql
tuple(T1, T2, ...)
```

Ejemplo de creación de una tupla:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## Trabajar con tipos de datos {#working-with-data-types}

Al crear una tupla sobre la marcha, ClickHouse detecta automáticamente el tipo de cada argumento como el mínimo de los tipos que pueden almacenar el valor del argumento. Si el argumento es [NULL](../../sql-reference/syntax.md#null-literal), el tipo del elemento de tupla es [NULL](nullable.md).

Ejemplo de detección automática de tipos de datos:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
