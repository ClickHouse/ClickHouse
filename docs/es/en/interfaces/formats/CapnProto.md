---
alias: []
description: 'Documentación sobre CapnProto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `CapnProto` es un formato de mensajes binario similar al formato [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) y a [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), a diferencia de [JSON](./JSON/JSON.md) o [MessagePack](https://msgpack.org/).
Los mensajes de CapnProto están fuertemente tipados y no son autodescriptivos, lo que significa que requieren la descripción de un esquema externo. El esquema se aplica dinámicamente y se almacena en caché para cada consulta.

Véase también [Format Schema](/es/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## Correspondencia entre tipos de datos
</div>

La siguiente tabla muestra los tipos de datos compatibles y cómo se corresponden con los [tipos de datos](/es/sql-reference/data-types/index.md) de ClickHouse en las consultas `INSERT` y `SELECT`.

| Tipo de dato de CapnProto (`INSERT`)                 | Tipo de dato de ClickHouse                                                                                                                             | Tipo de dato de CapnProto (`SELECT`)                 |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/es/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/es/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/es/sql-reference/data-types/int-uint.md), [Date](/es/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/es/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/es/sql-reference/data-types/int-uint.md), [DateTime](/es/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/es/sql-reference/data-types/int-uint.md), [Decimal32](/es/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/es/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/es/sql-reference/data-types/int-uint.md), [DateTime64](/es/sql-reference/data-types/datetime.md), [Decimal64](/es/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/es/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/es/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/es/sql-reference/data-types/string.md), [FixedString](/es/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/es/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/es/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/es/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/es/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/es/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/es/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/es/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/es/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/es/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* Los tipos enteros pueden convertirse entre sí durante la entrada y la salida.
* Para trabajar con `Enum` en formato CapnProto, utilice el ajuste [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/es/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode).
* Los arrays pueden anidarse y pueden tener un valor del tipo `Nullable` como argumento. Los tipos `Tuple` y `Map` también pueden anidarse.

<div id="example-usage">
  ## Ejemplo de uso
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### Insertar y seleccionar datos
</div>

Puede insertar datos de CapnProto desde un archivo en una tabla de ClickHouse mediante el siguiente comando:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Donde `schema.capnp` se ve así:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Puede seleccionar datos de una tabla de ClickHouse y guardarlos en un archivo con formato `CapnProto` mediante el siguiente comando:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### Uso de un esquema autogenerado
</div>

Si no tienes un esquema externo de `CapnProto` para tus datos, aún puedes escribir y leer datos en formato `CapnProto` usando un esquema autogenerado.

Por ejemplo:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

En este caso, ClickHouse generará automáticamente el esquema CapnProto según la estructura de la tabla mediante la función [structureToCapnProtoSchema](/es/sql-reference/functions/other-functions.md#structureToCapnProtoSchema), y usará este esquema para serializar los datos en formato CapnProto.

También puede leer un archivo CapnProto con un esquema autogenerado (en este caso, el archivo debe haberse creado con el mismo esquema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## Configuración de formato
</div>

La configuración [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) está habilitada de forma predeterminada y se aplica si no se ha establecido [`format_schema`](/es/interfaces/formats#formatschema).

También puede guardar el esquema autogenerado en un archivo durante la entrada/salida mediante la configuración [`output_format_schema`](/es/operations/settings/formats#output_format_schema).

Por ejemplo:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

En este caso, el esquema `CapnProto` autogenerado se guardará en el archivo `path/to/schema/schema.capnp`.