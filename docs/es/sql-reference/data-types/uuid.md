---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

Un identificador único universal (UUID) es un número de 16 bytes utilizado para identificar registros. Para obtener información detallada sobre el UUID, consulte [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

El ejemplo de valor de tipo UUID se representa a continuación:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

Si no especifica el valor de la columna UUID al insertar un nuevo registro, el valor UUID se rellena con cero:

``` text
00000000-0000-0000-0000-000000000000
```

## Cómo generar {#how-to-generate}

Para generar el valor UUID, ClickHouse proporciona el [GenerateUUIDv4](../../sql-reference/functions/uuid-functions.md) función.

## Ejemplo de uso {#usage-example}

**Ejemplo 1**

En este ejemplo se muestra la creación de una tabla con la columna de tipo UUID e insertar un valor en la tabla.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Ejemplo 2**

En este ejemplo, el valor de la columna UUID no se especifica al insertar un nuevo registro.

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restricción {#restrictions}

El tipo de datos UUID sólo admite funciones que [Cadena](string.md) tipo de datos también soporta (por ejemplo, [minuto](../../sql-reference/aggregate-functions/reference.md#agg_function-min), [máximo](../../sql-reference/aggregate-functions/reference.md#agg_function-max), y [contar](../../sql-reference/aggregate-functions/reference.md#agg_function-count)).

El tipo de datos UUID no es compatible con operaciones aritméticas (por ejemplo, [abdominales](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)) o funciones agregadas, tales como [resumir](../../sql-reference/aggregate-functions/reference.md#agg_function-sum) y [avg](../../sql-reference/aggregate-functions/reference.md#agg_function-avg).

[Artículo Original](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
