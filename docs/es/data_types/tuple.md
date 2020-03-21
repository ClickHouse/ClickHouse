# Tuple (T1, T2, …) {#tuplet1-t2}

Una tupla de elementos, cada uno con un individuo [tipo](index.md#data_types).

Las tuplas se utilizan para la agrupación temporal de columnas. Las columnas se pueden agrupar cuando se usa una expresión IN en una consulta y para especificar ciertos parámetros formales de las funciones lambda. Para obtener más información, consulte las secciones [IN operadores](../query_language/select.md) y [Funciones de orden superior](../query_language/functions/higher_order_functions.md).

Las tuplas pueden ser el resultado de una consulta. En este caso, para formatos de texto distintos de JSON, los valores están separados por comas entre corchetes. En formatos JSON, las tuplas se generan como matrices (entre corchetes).

## Creando una tupla {#creating-a-tuple}

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

Al crear una tupla sobre la marcha, ClickHouse detecta automáticamente el tipo de cada argumento como el mínimo de los tipos que pueden almacenar el valor del argumento. Si el argumento es [NULO](../query_language/syntax.md#null-literal), el tipo del elemento de tupla es [NULL](nullable.md).

Ejemplo de detección automática de tipos de datos:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/es/data_types/tuple/) <!--hide-->
