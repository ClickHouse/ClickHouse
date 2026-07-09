---
description: 'Documentación del tipo de dato Date en ClickHouse'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

Una fecha. Se almacena en dos bytes como el número de días transcurridos desde 1970-01-01 (sin signo). Permite almacenar valores desde poco después del inicio de la Unix epoch hasta el umbral superior definido por una constante en la fase de compilación (actualmente, hasta el año 2149, aunque el último año totalmente compatible es 2148).

Rango de valores admitidos: [1970-01-01, 2149-06-06].

El valor de fecha se almacena sin la zona horaria.

**Ejemplo**

Creación de una tabla con una columna de tipo `Date` e inserción de datos en ella:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**Véase también**

* [Funciones para trabajar con fechas y horas](../../sql-reference/functions/date-time-functions.md)
* [Operadores para trabajar con fechas y horas](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [tipo de dato `DateTime`](../../sql-reference/data-types/datetime.md)