# Matriz (T) {#data-type-array}

Matriz de `T`-tipo de artículos.

`T` puede ser cualquier cosa, incluida una matriz.

## Creación de una matriz {#creating-an-array}

Puede usar una función para crear una matriz:

``` sql
array(T)
```

También puede usar corchetes.

``` sql
[]
```

Ejemplo de creación de una matriz:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## Trabajar con tipos de datos {#working-with-data-types}

Al crear una matriz sobre la marcha, ClickHouse define automáticamente el tipo de argumento como el tipo de datos más estrecho que puede almacenar todos los argumentos enumerados. Si hay alguna [NULO](../query_language/syntax.md#null-literal) o [NULO](nullable.md#data_type-nullable) los argumentos de tipo, el tipo de elementos de la matriz es [NULO](nullable.md).

Si ClickHouse no pudo determinar el tipo de datos, generará una excepción. Por ejemplo, esto sucederá al intentar crear una matriz con cadenas y números simultáneamente (`SELECT array(1, 'a')`).

Ejemplos de detección automática de tipos de datos:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Si intenta crear una matriz de tipos de datos incompatibles, ClickHouse produce una excepción:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

[Artículo Original](https://clickhouse.tech/docs/es/data_types/array/) <!--hide-->
