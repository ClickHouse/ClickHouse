---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: 'NULL'
---

# Nivel de Cifrado WEP) {#data_type-nullable}

Permite almacenar marcador especial ([NULL](../../sql-reference/syntax.md)) que denota “missing value” junto con los valores normales permitidos por `TypeName`. Por ejemplo, un `Nullable(Int8)` tipo columna puede almacenar `Int8` valores de tipo, y las filas que no tienen un valor almacenarán `NULL`.

Para un `TypeName`, no puede usar tipos de datos compuestos [Matriz](array.md) y [Tupla](tuple.md). Los tipos de datos compuestos pueden contener `Nullable` valores de tipo, como `Array(Nullable(Int8))`.

A `Nullable` no se puede incluir en los índices de tabla.

`NULL` es el valor predeterminado para cualquier `Nullable` tipo, a menos que se especifique lo contrario en la configuración del servidor ClickHouse.

## Características de almacenamiento {#storage-features}

Almacenar `Nullable` en una columna de tabla, ClickHouse usa un archivo separado con `NULL` máscaras además del archivo normal con valores. Las entradas en el archivo de máscaras permiten ClickHouse distinguir entre `NULL` y un valor predeterminado del tipo de datos correspondiente para cada fila de la tabla. Debido a un archivo adicional, `Nullable` La columna consume espacio de almacenamiento adicional en comparación con una normal similar.

!!! info "Nota"
    Utilizar `Nullable` casi siempre afecta negativamente al rendimiento, tenga esto en cuenta al diseñar sus bases de datos.

## Ejemplo de uso {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
