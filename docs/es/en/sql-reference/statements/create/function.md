---
description: 'Documentación de FUNCTION'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - función definida por el usuario (UDF)'
doc_type: 'reference'
---

Crea una función definida por el usuario (UDF) a partir de una expresión lambda. La expresión debe estar compuesta por parámetros de función, constantes, operadores u otras llamadas a funciones.

**Sintaxis**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

Una función puede tener un número arbitrario de parámetros.

Existen algunas restricciones:

* El nombre de una función debe ser único entre las funciones definidas por el usuario y las funciones del sistema.
* No se permiten las funciones recursivas.
* Todas las variables que use una función deben especificarse en su lista de parámetros.

Si se incumple alguna de estas restricciones, se genera una excepción.

**Ejemplo**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

En la siguiente consulta, se llama a una [función condicional](../../../sql-reference/functions/conditional-functions.md) dentro de una función definida por el usuario:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

Reemplace una UDF existente:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

<div id="executable-udfs">
  ### [UDF ejecutables](/es/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [Funciones definidas por el usuario en ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
