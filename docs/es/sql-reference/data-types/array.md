---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: Matriz (T)
---

# Matriz (t) {#data-type-array}

Una matriz de `T`-tipo de artículos. `T` puede ser cualquier tipo de datos, incluida una matriz.

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

Al crear una matriz sobre la marcha, ClickHouse define automáticamente el tipo de argumento como el tipo de datos más estrecho que puede almacenar todos los argumentos enumerados. Si hay alguna [NULL](nullable.md#data_type-nullable) o literal [NULL](../../sql-reference/syntax.md#null-literal) valores, el tipo de un elemento de matriz también se convierte en [NULL](nullable.md).

Si ClickHouse no pudo determinar el tipo de datos, genera una excepción. Por ejemplo, esto sucede cuando se intenta crear una matriz con cadenas y números simultáneamente (`SELECT array(1, 'a')`).

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

[Artículo Original](https://clickhouse.tech/docs/en/data_types/array/) <!--hide-->
