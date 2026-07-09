---
description: 'Documentación del modificador de tipo de dato Nullable en ClickHouse'
sidebar_label: 'Nullable(T)'
sidebar_position: 44
slug: /sql-reference/data-types/nullable
title: 'Nullable(T)'
doc_type: 'reference'
---

Permite almacenar un marcador especial ([NULL](../../sql-reference/syntax.md)) que denota un &quot;valor ausente&quot; junto con los valores normales permitidos por `T`. Por ejemplo, una columna de tipo `Nullable(Int8)` puede almacenar valores de tipo `Int8`, y las filas que no tienen valor almacenarán `NULL`.

`T` no puede ser ninguno de los siguientes tipos de datos compuestos:

* [Array](../../sql-reference/data-types/array.md) — No es compatible
* [Map](../../sql-reference/data-types/map.md) — No es compatible
* [Tuple](../../sql-reference/data-types/tuple.md) — Compatibilidad beta disponible*

Sin embargo, los tipos de datos compuestos **pueden contener** valores de tipo `Nullable`, por ejemplo, `Array(Nullable(Int8))` o `Tuple(Nullable(String), Nullable(Int64))`.

:::note Beta: Tuples Nullable

* [Nullable(Tuple(...))](../../sql-reference/data-types/tuple.md#nullable-tuple) es compatible cuando `enable_nullable_tuple_type = 1` está habilitado.
  :::

Un campo de tipo `Nullable` no puede incluirse en los índices de una tabla.

`NULL` es el valor predeterminado para cualquier tipo `Nullable`, a menos que se especifique lo contrario en la configuración del servidor ClickHouse.

<div id="storage-features">
  ## Características de almacenamiento
</div>

Para almacenar valores de tipo `Nullable` en una columna de una tabla, ClickHouse utiliza un archivo independiente con máscaras `NULL`, además del archivo normal con los valores. Las entradas del archivo de máscaras permiten a ClickHouse distinguir entre `NULL` y el valor predeterminado del tipo de dato correspondiente para cada fila de la tabla. Debido a este archivo adicional, una columna `Nullable` consume más espacio de almacenamiento que una columna normal similar.

:::note
El uso de `Nullable` casi siempre afecta negativamente al rendimiento; tenlo en cuenta al diseñar tus bases de datos.
:::

<div id="finding-null">
  ## Cómo encontrar NULL
</div>

Es posible encontrar valores `NULL` en una columna usando la subcolumna `null` sin leer toda la columna. Devuelve `1` si el valor correspondiente es `NULL` y `0` en caso contrario.

**Ejemplo**

```sql title="Query"
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

```text title="Response"
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

<div id="usage-example">
  ## Ejemplo de uso
</div>

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

```sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

```sql
SELECT x + y FROM t_null
```

```text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```