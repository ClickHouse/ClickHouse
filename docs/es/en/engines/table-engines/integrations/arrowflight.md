---
description: 'El motor permite consultar e insertar en conjuntos de datos remotos mediante el protocolo Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'Motor de tabla ArrowFlight'
doc_type: 'reference'
---

El motor de tabla ArrowFlight permite a ClickHouse leer de y escribir en conjuntos de datos remotos mediante el protocolo [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
Esta integración permite a ClickHouse interactuar con servidores externos compatibles con Flight en formato Arrow columnar y con alto rendimiento.

<div id="creating-a-table">
  ## Creación de una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Parámetros del motor**

* `host:port` — Dirección del servidor Arrow Flight remoto. Si se omite el puerto, se usa el puerto predeterminado `8815`. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Identificador del conjunto de datos en el servidor de Flight (se usa como descriptor PATH o en una consulta `SELECT *`, según la configuración `arrow_flight_request_descriptor_type`). [String](../../../sql-reference/data-types/string.md).
* `username` — Nombre de usuario para la autenticación HTTP básica. [String](../../../sql-reference/data-types/string.md).
* `password` — Contraseña para la autenticación HTTP básica. [String](../../../sql-reference/data-types/string.md).

Si se omiten `username` y `password`, no se usa autenticación (esto solo funciona si el servidor Arrow Flight permite el acceso sin autenticación).

La lista de columnas es opcional; si se omite, el esquema se infiere del servidor Arrow Flight remoto mediante `GetSchema`.

<div id="named-collections">
  ## Colecciones con nombre
</div>

El motor admite [colecciones con nombre](/es/operations/named-collections) para almacenar parámetros de conexión:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Parámetros de la colección con nombre:

| Parámetro                  | Obligatorio                         | Valor predeterminado | Descripción                                                                |
| -------------------------- | ----------------------------------- | -------------------- | -------------------------------------------------------------------------- |
| `host` o `hostname`        | No                                  | `""`                 | Nombre de host del servidor.                                               |
| `port`                     | Sí                                  | —                    | Puerto del servidor.                                                       |
| `dataset`                  | No                                  | `""`                 | Nombre del dataset o descriptor.                                           |
| `use_basic_authentication` | No                                  | `true`               | Habilita la autenticación básica.                                          |
| `user` o `username`        | Si la autenticación está habilitada | —                    | Nombre de usuario para la autenticación.                                   |
| `password`                 | No                                  | `""`                 | Contraseña para la autenticación.                                          |
| `enable_ssl`               | No                                  | `false`              | Habilita el cifrado TLS.                                                   |
| `ssl_ca`                   | No                                  | `""`                 | Ruta al archivo del certificado de CA para la verificación TLS.            |
| `ssl_override_hostname`    | No                                  | `""`                 | Sobrescribe el nombre de host que se verifica durante la verificación TLS. |

<div id="settings">
  ## Configuración
</div>

* `arrow_flight_request_descriptor_type` — Controla cómo se envía el nombre del conjunto de datos al servidor de Flight. Valores posibles: `path` (predeterminado; se envía como un descriptor PATH) o `command` (se envía como un descriptor CMD con `SELECT * FROM <dataset>`). Use `command` con servidores de Flight que esperan comandos SQL (p. ej., Dremio).

<div id="usage-example">
  ## Ejemplo de uso
</div>

Lectura de datos de un servidor Arrow Flight remoto:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserción de datos en un servidor Arrow Flight remoto:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## Notas
</div>

* Si se especifican columnas en la sentencia `CREATE TABLE`, deben coincidir con el esquema devuelto por el servidor de Flight.
* Si se omiten las columnas, el esquema se infiere automáticamente del servidor remoto.
* Se admiten tanto la lectura (`SELECT`) como la escritura (`INSERT`).
* La configuración `arrow_flight_request_descriptor_type` controla si el nombre del dataset se envía como un descriptor PATH o como un descriptor CMD que encapsula una consulta `SELECT *`.

<div id="see-also">
  ## Véase también
</div>

* [función de tabla arrowFlight](/es/sql-reference/table-functions/arrowflight)
* [interfaz Arrow Flight](/es/interfaces/arrowflight)
* [especificación de Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [formato Arrow en ClickHouse](/es/interfaces/formats/Arrow)