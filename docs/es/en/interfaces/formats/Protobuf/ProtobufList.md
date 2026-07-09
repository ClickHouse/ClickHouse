---
alias: []
description: 'Documentación del formato ProtobufList'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
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

El formato `ProtobufList` es similar al formato [`Protobuf`](./Protobuf.md), pero las filas se representan como una secuencia de submensajes incluidos en un mensaje con el nombre fijo &quot;Envelope&quot;.

<div id="example-usage">
  ## Ejemplo de uso
</div>

Por ejemplo:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

Donde el archivo `schemafile.proto` tiene este aspecto:

```capnp title="schemafile.proto"
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

El tipo de mensaje especificado en `format_schema` se busca primero como un tipo anidado dentro de un mensaje `Envelope` de nivel superior. Si no se encuentra ninguna coincidencia allí —ya sea porque el esquema no tiene un mensaje `Envelope` o porque `Envelope` no contiene un mensaje con el nombre solicitado—, se usa directamente el mensaje de nivel superior con ese nombre.

<div id="format-settings">
  ## Configuración de formato
</div>
