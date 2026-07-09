---
description: 'Documentación para el tipo de dato Date32 en ClickHouse, que almacena fechas
  con un rango ampliado en comparación con Date'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'referencia'
---

Una fecha. Admite el mismo intervalo de fechas que [DateTime64](../../sql-reference/data-types/datetime64.md). Se almacena como un entero con signo de 32 bits en orden de bytes nativo, donde el valor representa los días transcurridos desde `1900-01-01`. **¡Importante!** 0 representa `1970-01-01`, y los valores negativos representan los días anteriores a `1970-01-01`.

**Ejemplos**

Crear una tabla con una columna de tipo `Date32` e insertar datos en ella:

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**Véase también**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/es/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/es/sql-reference/functions/type-conversion-functions#toDate32OrNull)