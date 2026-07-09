---
description: 'CoalescingMergeTree hereda del motor MergeTree. Su característica clave
  es la capacidad de almacenar automáticamente el último valor no nulo de cada columna durante las fusiones de partes.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'Motor de tabla CoalescingMergeTree'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note Disponible a partir de la versión 25.6
Este motor de tabla está disponible a partir de la versión 25.6 en adelante tanto en OSS como en Cloud.
:::

Este motor hereda de [MergeTree](/es/engines/table-engines/mergetree-family/mergetree). La principal diferencia está en cómo se fusionan las partes de datos: en las tablas `CoalescingMergeTree`, ClickHouse reemplaza todas las filas con la misma clave primaria (o, más precisamente, la misma [clave de ordenación](../../../engines/table-engines/mergetree-family/mergetree.md)) por una única fila que contiene los valores no NULL más recientes de cada columna.

Esto permite realizar upserts a nivel de columna, lo que significa que puedes actualizar solo columnas específicas en lugar de filas completas.

`CoalescingMergeTree` está pensado para usarse con tipos Nullable en columnas que no forman parte de la clave. Si las columnas no son Nullable, el comportamiento es el mismo que con [ReplacingMergeTree](/es/engines/table-engines/mergetree-family/replacingmergetree).

<div id="creating-a-table">
  ## Creación de una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de la solicitud, consulte la [descripción de la solicitud](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-coalescingmergetree">
  ### Parámetros de CoalescingMergeTree
</div>

<div id="columns">
  #### Columnas
</div>

`columns` - Opcional. Una tupla con los nombres de las columnas cuyos valores se combinarán. Las columnas proporcionadas no deben formar parte de la partición ni de la clave de ordenación. Si no se especifica `columns`, ClickHouse combina los valores de todas las columnas que no forman parte de la clave de ordenación.

<div id="query-clauses">
  ### Cláusulas de consulta
</div>

Al crear una tabla `CoalescingMergeTree`, se requieren las mismas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) que al crear una tabla `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos y, si es posible, migre los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Todos los parámetros, excepto `columns`, tienen el mismo significado que en `MergeTree`.

  * `columns` — tupla con los nombres de las columnas cuyos valores se sumarán. Es un parámetro opcional. Para ver una descripción, consulte el texto anterior.
</details>

<div id="usage-example">
  ## Ejemplo de uso
</div>

Considere la siguiente tabla:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

Inserte datos en ella:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

El resultado se verá así:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

Consulta recomendada para obtener un resultado correcto y definitivo:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

Usar el modificador `FINAL` obliga a ClickHouse a aplicar la lógica de fusión en el momento de la consulta, lo que garantiza que obtengas el valor &quot;más reciente&quot; correcto y consolidado para cada columna. Este es el método más seguro y preciso al consultar una tabla CoalescingMergeTree.

:::note

Un enfoque con `GROUP BY` puede devolver resultados incorrectos si las partes subyacentes no se han fusionado por completo.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Agregación de elementos de Tuple
</div>

Cuando la configuración `allow_tuple_element_aggregation` está habilitada, las columnas `Tuple` se aplanan de forma recursiva para que cada elemento hoja participe en la consolidación de manera independiente. Esto le permite almacenar varios campos en una sola columna `Tuple` y hacer que se consoliden elemento por elemento durante las fusiones; cada sub-columna `Nullable` conserva de forma independiente el valor no `NULL` más reciente.

A las sub-columnas aplanadas se les aplican las mismas reglas que a las columnas normales:

* Las sub-columnas que pertenecen a un `Tuple` de la clave de ordenación o de la clave de partición se excluyen de la consolidación.
* Si se especifica `columns`, solo se consolidan las sub-columnas de las columnas `Tuple` indicadas.

:::note
Esta configuración es inmutable y debe especificarse al crear la tabla.
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```