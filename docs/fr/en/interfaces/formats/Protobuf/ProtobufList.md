---
alias: []
description: 'Documentation sur le format ProtobufList'
input_format: true
keywords: ['ProtobufList']
output_format: true
slug: /interfaces/formats/ProtobufList
title: 'ProtobufList'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `ProtobufList` est similaire au format [`Protobuf`](./Protobuf.md), mais les lignes y sont représentées sous forme d’une séquence de sous-messages contenus dans un message portant le nom fixe « Envelope ».

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Par exemple :

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

Le fichier `schemafile.proto` se présente ainsi :

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

Le type de message spécifié dans `format_schema` est déterminé en le recherchant d’abord comme type imbriqué dans un message `Envelope` de premier niveau. Si aucune correspondance n’y est trouvée — soit parce que le schéma ne comporte pas de message `Envelope`, soit parce que `Envelope` ne contient pas de message portant le nom demandé — le message de premier niveau portant ce nom est utilisé directement.

<div id="format-settings">
  ## Paramètres de format
</div>
