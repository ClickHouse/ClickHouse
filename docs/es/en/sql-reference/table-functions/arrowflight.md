---
description: 'Permite leer y escribir en datos expuestos a través de un servidor Apache Arrow Flight.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

Permite leer y escribir en datos expuestos a través de un servidor [Apache Arrow Flight](/es/interfaces/arrowflight).

**Sintaxis**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Argumentos**

* `host:port` — Dirección del servidor Arrow Flight. Si se omite el puerto, se utiliza el puerto predeterminado `8815`. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Nombre del conjunto de datos o descriptor disponible en el servidor Arrow Flight. [String](../../sql-reference/data-types/string.md).
* `username` — Nombre de usuario para la autenticación HTTP básica. [String](../../sql-reference/data-types/string.md).
* `password` — Contraseña para la autenticación HTTP básica. [String](../../sql-reference/data-types/string.md).

Si no se especifican `username` y `password`, no se utiliza autenticación (esto solo funciona si el servidor Arrow Flight permite el acceso sin autenticación).

La función también admite [colecciones con nombre](/es/operations/named-collections); consulta el [motor de tabla ArrowFlight](/es/engines/table-engines/integrations/arrowflight#named-collections) para ver la lista de parámetros compatibles.

**Valor devuelto**

Un objeto de tabla que representa el conjunto de datos remoto. El esquema se infiere del servidor Arrow Flight.

**Configuración**

* `arrow_flight_request_descriptor_type` — Controla cómo se envía el nombre del conjunto de datos al servidor de Flight. Valores: `path` (predeterminado) o `command`. Consulta el [motor de tabla ArrowFlight](/es/engines/table-engines/integrations/arrowflight#settings) para obtener más información.

**Ejemplos**

Lectura desde un servidor Arrow Flight remoto:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserción de datos en un servidor Arrow Flight remoto:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Uso de una colección con nombre:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**Véase también**

* [Motor de tabla ArrowFlight](/es/engines/table-engines/integrations/arrowflight)
* [Interfaz Arrow Flight](/es/interfaces/arrowflight)
* [Especificación de Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)