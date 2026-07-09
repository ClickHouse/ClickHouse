---
description: 'Documentación del tipo de dato UUID en ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

Un identificador único universal (UUID) es un valor de 16 bytes que se utiliza para identificar registros. Para obtener información detallada sobre los UUIDs, consulta [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

Aunque existen distintas variantes de UUID, por ejemplo UUIDv4 y UUIDv7 (consulta [aquí](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse no valida que los UUIDs insertados se ajusten a ninguna variante en particular.
Internamente, los UUIDs se tratan como una secuencia de 16 bytes aleatorios con representación [8-4-4-4-12](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) en el nivel SQL.

Valor de UUID de ejemplo:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

El UUID predeterminado es todo de ceros. Se utiliza, por ejemplo, cuando se inserta un nuevo registro pero no se especifica ningún valor para una columna de tipo UUID:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
Por razones históricas, los UUIDs se ordenan por su segunda mitad.

Aunque esto no supone ningún problema para los valores UUIDv4, puede degradar el rendimiento con columnas UUIDv7 usadas en definiciones de índice primario (su uso en claves de ordenación o claves de partición no presenta problemas).
Más concretamente, los valores UUIDv7 constan de una marca de tiempo en la primera mitad y de un contador en la segunda mitad.
Por lo tanto, la ordenación de UUIDv7 en índices de clave primaria dispersos (es decir, los primeros valores de cada granularidad de índice) se hará según el campo contador.
Suponiendo que los UUIDs se ordenaran por la primera mitad (marca de tiempo), se espera que el paso de análisis del índice de clave primaria al inicio de las consultas descarte todas las marcas en todas las partes salvo una.
Sin embargo, con la ordenación por la segunda mitad (contador), se espera que se devuelva al menos una marca para todas las partes, lo que provoca accesos innecesarios al disco.
:::

Ejemplo:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

Como solución alternativa, el UUID puede convertirse en una marca de tiempo obtenida de la segunda mitad:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

Resultado (si se insertan los mismos datos):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## Generación de UUIDs
</div>

ClickHouse proporciona la función [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) para generar valores aleatorios de UUIDv4.

<div id="usage-example">
  ## Ejemplo de uso
</div>

**Ejemplo 1**

Este ejemplo muestra cómo crear una tabla con una columna UUID e insertar un valor en la tabla.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Ejemplo 2**

En este ejemplo, no se especifica ningún valor para la columna UUID al insertar el registro; es decir, se inserta el valor UUID predeterminado:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## Restricciones
</div>

El tipo de dato UUID solo admite las funciones que también admite el tipo de datos [String](../../sql-reference/data-types/string.md) (por ejemplo, [min](/es/sql-reference/aggregate-functions/reference/min), [max](/es/sql-reference/aggregate-functions/reference/max) y [count](/es/sql-reference/aggregate-functions/reference/count)).

El tipo de dato UUID no es compatible con las operaciones aritméticas (por ejemplo, [abs](/es/sql-reference/functions/arithmetic-functions#abs)) ni con las funciones de agregación, como [sum](/es/sql-reference/aggregate-functions/reference/sum) y [avg](/es/sql-reference/aggregate-functions/reference/avg).